#!/usr/bin/env python
import os
import logging
from logging.config import dictConfig
import json
import datetime as dt

import asyncio
import aiohttp
import aiopg

from prometheus_client.parser import text_string_to_metric_families
from .cloudfoundry import get_client

from dotenv import load_dotenv
load_dotenv()

class InsufficientData(Exception):
    pass


TRUTHY_VALUES = ['on', 'yes', 'true', 'True', '1']

DEBUG = os.getenv('DEBUG', False) in TRUTHY_VALUES

PROM_PAAS_EXPORTER_URL = os.getenv('PROM_PAAS_EXPORTER_URL')
PROM_PAAS_EXPORTER_USERNAME = os.getenv('PROM_PAAS_EXPORTER_USERNAME')
PROM_PAAS_EXPORTER_PASSWORD = os.getenv('PROM_PAAS_EXPORTER_PASSWORD')
SCRAPE_INTERVAL_SECONDS = int(os.getenv('SCRAPE_INTERVAL_SECONDS', 5))
APP_CHECK_INTERVAL_SECONDS = int(os.getenv('APP_CHECK_INTERVAL_SECONDS', 30))
POSTGRES_DSN = os.getenv('POSTGRES_DSN')
CF_USERNAME = os.getenv('CF_USERNAME')
CF_PASSWORD = os.getenv('CF_PASSWORD')
SLACK_URL = os.getenv('SLACK_URL')
DEFAULT_HIGH_THRESHOLD_CPU_PERCENTAGE = int(os.getenv('DEFAULT_HIGH_THRESHOLD_CPU_PERCENTAGE', 50))
DEFAULT_LOW_THRESHOLD_CPU_PERCENTAGE = int(os.getenv('DEFAULT_LOW_THRESHOLD_CPU_PERCENTAGE', 10))
DEFAULT_THRESHOLD_PERIOD_MINUTES = int(os.getenv('DEFAULT_THRESHOLD_PERIOD_MINUTES', 5))
DEFAULT_MINIMUM_INSTANCES = int(os.getenv('DEFAULT_MINIMUM_INSTANCES', 2))
DEFAULT_MAXIMUM_INSTANCES = int(os.getenv('DEFAULT_MAXIMUM_INSTANCES', 10))
DEFAULT_COOLDOWN_PERIOD_MINUTES = int(os.getenv('DEFAULT_COOLDOWN_PERIOD_MINUTES', 5))
ENABLE_VERBOSE_OUTPUT = os.getenv('ENABLE_VERBOSE_OUTPUT', 'False') in TRUTHY_VALUES


dictConfig({
    'version': 1,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'default',
            'stream': 'ext://sys.stdout'
        }
    },
    'formatters': {
        'default': {
            'format': '%(asctime)s %(levelname)-8s %(name)-15s %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': True
        },
        'cf-autoscaler': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False
        },
    }
})

logger = logging.getLogger('cf-autoscaler')


def get_cpu_metrics(prom_exporter_text):
    for family in text_string_to_metric_families(prom_exporter_text):
        for sample in family.samples:
            # we're only interested in CPU metrics from web apps
            if sample[0] == 'cpu' and '__conduit' not in sample[1]['app']:
                yield (
                    sample[0],                   # metric
                    sample[1]['app'],            # app
                    int(sample[1]['instance']),  # instance
                    sample[1]['space'],          # space
                    sample[2],                   # value
                )


def get_enabled_apps(cf_client):
    """Scan all CF apps and return a list of enabled apps"""

    enabled_apps = []
    kwargs = {'results-per-page': 15}
    for app in cf_client.apps.list(**kwargs):
        app_conf = app['entity']['environment_json'] or {}
        status = app_conf.get('AUTOSCALING', 'False')

        if status in TRUTHY_VALUES:
            enabled_apps.append(app)

    return enabled_apps


def get_autoscaling_params(cf_app):
    """Determine the autoscaling parameters for an individual app"""

    app_conf = cf_app['entity']['environment_json'] or {}

    enabled = app_conf.get('AUTOSCALING', 'False') in TRUTHY_VALUES

    return {
        'enabled': enabled,
        'min_instances': int(app_conf.get('AUTOSCALING_MIN', DEFAULT_MINIMUM_INSTANCES)),
        'max_instances': int(app_conf.get('AUTOSCALING_MAX', DEFAULT_MAXIMUM_INSTANCES)),
        'instances': int(cf_app['entity']['instances']),
        'threshold_period': DEFAULT_THRESHOLD_PERIOD_MINUTES,
        'high_threshold': DEFAULT_HIGH_THRESHOLD_CPU_PERCENTAGE,
        'low_threshold': DEFAULT_LOW_THRESHOLD_CPU_PERCENTAGE,
        'cooldown': DEFAULT_COOLDOWN_PERIOD_MINUTES,
    }


async def notify(app_name, message, is_verbose=False):

    _logger = logger.debug if is_verbose else logger.info

    _logger(f'{app_name}: {message}')

    if SLACK_URL:
        if not is_verbose or is_verbose and ENABLE_VERBOSE_OUTPUT:
            await slack_notify(app_name, message)


async def slack_notify(app_name, message):

    time = dt.datetime.now().strftime('%H:%M:%S')

    slack_message = json.dumps(
        {
            'text': f'[{time}] *{app_name}*: `{message}`',
            'username': 'autoscalingbot',
            'mrkdwn': True
        }
    ).encode()

    async with aiohttp.ClientSession() as session:
        async with session.post(SLACK_URL, data=slack_message) as resp:
            if resp.status != 200:
                logger.error('Slack message sending error: %s %s', resp.status, await resp.text())


async def is_cooldown(app_name, space_name, cooldown, conn):
    """Returns `True` if the current time is within the cooldown period"""
    stmt = "SELECT COUNT(*) FROM actions WHERE app=%s and space=%s" \
           "AND timestamp > now() - INTERVAL '%s min';"

    async with conn.cursor() as cur:
        await cur.execute(stmt, (app_name,space_name,cooldown,))

        result = await cur.fetchone()
        return result[0] != 0


async def scale(app, space_name, instances, conn):
    stmt = 'INSERT INTO actions (timestamp, app, space, instances) VALUES(now(), %s, %s, %s);'

    # The API does not provide a public update method, so we're forced to use
    # the private _update method instead.
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, app.client.apps._update(app['metadata']['guid'], dict(instances=instances)))

    async with conn.cursor() as cur:
        await cur.execute(stmt, (app['entity']['name'], space_name, instances))


async def get_avg_cpu(app_name, space_name, period, conn):

    stmt = "SELECT count(*), avg(value), max(instance) + 1 FROM metrics " \
           "WHERE app=%s AND space=%s AND " \
           "timestamp BETWEEN now() - INTERVAL '%s min' AND now() " \
           "GROUP BY app;"

    async with conn.cursor() as cur:
        await cur.execute(stmt, (app_name, space_name, period,))

        if cur.rowcount == 0:
            raise InsufficientData

        datapoints, avg_cpu, instance_count = await cur.fetchone()

        # Do we have enough data points spanning the threshold period to make a decision?
        # This will probably need some additional work
        if datapoints > (period * 60 / SCRAPE_INTERVAL_SECONDS) * instance_count * 0.7:
            return avg_cpu

        raise InsufficientData


async def get_metrics(conn):
    """Get app metrics from the prometheus exporter and store in database"""

    if PROM_PAAS_EXPORTER_USERNAME:
        auth = aiohttp.helpers.BasicAuth(PROM_PAAS_EXPORTER_USERNAME, PROM_PAAS_EXPORTER_PASSWORD, )
    else:
        auth = aiohttp.helpers.BasicAuth()

    async def fetch(session, url):
        async with session.get(url) as response:
            return await response.text()

    async with aiohttp.ClientSession(auth=auth) as session:
        metrics = await fetch(session, PROM_PAAS_EXPORTER_URL)

    stmt = 'INSERT INTO metrics (timestamp, metric, app, instance, space, value) ' \
           'VALUES(now(), %s, %s, %s, %s, %s);'

    async with conn.cursor() as cur:
        for values in get_cpu_metrics(metrics):
            await cur.execute(stmt, values)


async def check_app_autoscaling_state(conn):
    loop = asyncio.get_event_loop()
    client = await loop.run_in_executor(None, get_client, CF_USERNAME, CF_PASSWORD)
    enabled_apps = await loop.run_in_executor(None, get_enabled_apps, client)

    for app in enabled_apps:
        app_name = app['entity']['name']
        space = await loop.run_in_executor(None, app.space)
        space_name = space['entity']['name']
        params = get_autoscaling_params(app)

        if await is_cooldown(app_name, space_name, params['cooldown'], conn):
            await notify(app_name, 'is in cool down period', is_verbose=True)
            continue

        try:
            average_cpu = await get_avg_cpu(app_name, space_name, params['threshold_period'], conn)
        except InsufficientData:
            await notify(app_name, 'insufficient data', is_verbose=True)
            continue

        if params['low_threshold'] < average_cpu < params['high_threshold']:
            await notify(app_name, f'is within bounds - current: {average_cpu}', is_verbose=True)
            continue

        if average_cpu > params['high_threshold']:
            if params['instances'] >= params['max_instances']:
                await notify(app_name, 'cannot scale up - already at max', is_verbose=True)
                continue

            new_instance_count = params['instances'] + 1

        elif average_cpu < params['low_threshold']:
            if params['instances'] <= params['min_instances']:
                await notify(app_name, 'cannot scale down already at min', is_verbose=True)
                continue

            new_instance_count = params['instances'] - 1

        await scale(app, space_name, new_instance_count, conn)

        await notify(app_name,
                     f'scaled from {params["instances"]} to {new_instance_count} instances - avg cpu {average_cpu}')


async def periodic_check_apps(pool):
    """Periodically scan apps, check autoscaling config and status"""
    while True:
        async with pool.acquire() as conn:
            logger.info('checking app autoscaling status')
            await check_app_autoscaling_state(conn)
        await asyncio.sleep(APP_CHECK_INTERVAL_SECONDS)


async def periodic_get_metrics(pool):
    """Periodically scrape app metrics from the prometheus exporter"""

    while True:
        async with pool.acquire() as conn:
            logger.info('retrieving metrics')
            await get_metrics(conn)
        await asyncio.sleep(SCRAPE_INTERVAL_SECONDS)


async def periodic_remove_old_data(pool):
    while True:
        logger.info('removing old data')
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                cur.execute("DELETE FROM metrics where timestamp < now() - INTERVAL '20 min';")
        await asyncio.sleep(300)


async def main():
    loop = asyncio.get_event_loop()

    await notify('general', 'autoscaler started', is_verbose=True)

    async with aiopg.create_pool(POSTGRES_DSN) as pool:
        await asyncio.gather(
            loop.create_task(periodic_get_metrics(pool)),
            loop.create_task(periodic_check_apps(pool)),
            loop.create_task(periodic_remove_old_data(pool)))


def custom_exception_handler(loop, context):
    loop.default_exception_handler(context)

    print(context)
    loop.stop()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.set_debug(DEBUG)
    loop.set_exception_handler(custom_exception_handler)
    loop.create_task(main())
    loop.run_forever()
