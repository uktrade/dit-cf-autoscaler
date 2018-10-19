#!/usr/bin/env python

from functools import partial

import os
import logging
from logging.config import dictConfig
import json
import datetime as dt

import asyncio
import aiohttp
from aiohttp import web
from aioprometheus import Counter, Gauge, Service, Summary, timer
import aiopg

from raven import Client
from raven_aiohttp import QueuedAioHttpTransport

from prometheus_client.parser import text_string_to_metric_families
from .cloudfoundry import get_client

from dotenv import load_dotenv
load_dotenv()


class InsufficientData(Exception):
    pass


TRUTHY_VALUES = ['on', 'yes', 'true', 'True', '1']

DEBUG = os.getenv('DEBUG', 'False') in TRUTHY_VALUES

PORT = int(os.getenv('PORT', 8080))
PROM_PAAS_EXPORTER_URL = os.getenv('PROM_PAAS_EXPORTER_URL')
PROM_PAAS_EXPORTER_USERNAME = os.getenv('PROM_PAAS_EXPORTER_USERNAME')
PROM_PAAS_EXPORTER_PASSWORD = os.getenv('PROM_PAAS_EXPORTER_PASSWORD')
SCRAPE_INTERVAL_SECONDS = int(os.getenv('SCRAPE_INTERVAL_SECONDS', 5))
APP_CHECK_INTERVAL_SECONDS = int(os.getenv('APP_CHECK_INTERVAL_SECONDS', 30))
DATABASE_URL = os.getenv('DATABASE_URL')
CF_USERNAME = os.getenv('CF_USERNAME')
CF_PASSWORD = os.getenv('CF_PASSWORD')
SLACK_URL = os.getenv('SLACK_URL')
DEFAULT_HIGH_THRESHOLD_CPU_PERCENTAGE = int(os.getenv('DEFAULT_HIGH_THRESHOLD_CPU_PERCENTAGE', 50))
DEFAULT_LOW_THRESHOLD_CPU_PERCENTAGE = int(os.getenv('DEFAULT_LOW_THRESHOLD_CPU_PERCENTAGE', 10))
DEFAULT_THRESHOLD_PERIOD_MINUTES = int(os.getenv('DEFAULT_THRESHOLD_PERIOD_MINUTES', 1))
DEFAULT_MINIMUM_INSTANCES = int(os.getenv('DEFAULT_MINIMUM_INSTANCES', 2))
DEFAULT_MAXIMUM_INSTANCES = int(os.getenv('DEFAULT_MAXIMUM_INSTANCES', 10))
DEFAULT_SCALE_UP_DELAY_MINUTES = int(os.getenv('DEFAULT_SCALE_UP_DELAY_MINUTES', 1))
DEFAULT_SCALE_DOWN_DELAY_MINUTES = int(os.getenv('DEFAULT_SCALE_DOWN_DELAY_MINUTES', 2))
SENTRY_DSN = os.getenv('SENTRY_DSN')

# prometheus metrics:
PROM_GET_METRICS_TIME = Summary('get_metrics', 'the time it takes to get application metrics')
PROM_AUTOSCALER_CHECK_TIME = Summary('autoscaler_check_time', 'the time it takes for the autoscaler check to complete')
PROM_SCALING_ACTIONS = Counter('scaling_actions', 'a count of the number of scaling actions that have taken place')
PROM_AUTOSCALING_ENABLED = Gauge('autoscaling_enabled', 'number of apps that have autoscaling enabled')
PROM_INSUFFICIENT_DATA = Gauge('insufficient_data', 'number of apps that have insufficient data')


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
                yield {
                    'app': sample[1]['app'],
                    'instance': int(sample[1]['instance']),
                    'space': sample[1]['space'],
                    'value': sample[2],
                }


def group_cpu_metrics(metrics):
    """Group together each instances metrics per app/space"""
    grouped_metrics = {}
    for metric in metrics:
        grouped_metrics.setdefault(metric['space'], {}).setdefault(metric['app'], []).append(metric['value'])

    return [(app, space, metric_values,)
            for space, apps in grouped_metrics.items() for app, metric_values in apps.items()]


def get_enabled_apps(cf_client):
    """Scan all CF apps and return a list of enabled apps"""

    enabled_apps = []
    kwargs = {'results-per-page': 15}
    for app in cf_client.apps.list(**kwargs):
        app_conf = app['entity']['environment_json'] or {}
        status = app_conf.get('X_AUTOSCALING', 'False')

        if status in TRUTHY_VALUES:
            enabled_apps.append(app)

    return enabled_apps


def get_autoscaling_params(cf_app):
    """Determine the autoscaling parameters for an individual app"""

    app_conf = cf_app['entity']['environment_json'] or {}

    enabled = app_conf.get('X_AUTOSCALING', 'False') in TRUTHY_VALUES

    return {
        'enabled': enabled,
        'min_instances': int(app_conf.get('X_AUTOSCALING_MIN', DEFAULT_MINIMUM_INSTANCES)),
        'max_instances': int(app_conf.get('X_AUTOSCALING_MAX', DEFAULT_MAXIMUM_INSTANCES)),
        'instances': int(cf_app['entity']['instances']),
        'threshold_period': DEFAULT_THRESHOLD_PERIOD_MINUTES,
        'high_threshold': DEFAULT_HIGH_THRESHOLD_CPU_PERCENTAGE,
        'low_threshold': DEFAULT_LOW_THRESHOLD_CPU_PERCENTAGE,
        'scale_up_delay': DEFAULT_SCALE_UP_DELAY_MINUTES,
        'scale_down_delay': DEFAULT_SCALE_DOWN_DELAY_MINUTES,
    }


async def notify(app_name, message):

    logger.info(f'{app_name}: {message}')

    if SLACK_URL:
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
        await cur.execute(stmt, (app_name, space_name, cooldown,))

        result = await cur.fetchone()
        return result[0] != 0


async def scale(app, space_name, instances, conn):
    stmt = 'INSERT INTO actions (timestamp, app, space, instances) VALUES(now(), %s, %s, %s);'

    # The API does not provide a public update method, so we're forced to use
    # the private _update method instead.
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, app.client.apps._update, app['metadata']['guid'], dict(instances=instances))

    async with conn.cursor() as cur:
        await cur.execute(stmt, (app['entity']['name'], space_name, instances))


async def get_avg_cpu(app_name, space_name, period, conn):

    stmt = "SELECT count(*), avg(average_cpu) FROM metrics " \
           "WHERE app=%s AND space=%s AND " \
           "timestamp BETWEEN now() - INTERVAL '%s min' AND now() " \
           "GROUP BY app;"

    async with conn.cursor() as cur:
        await cur.execute(stmt, (app_name, space_name, period,))

        if cur.rowcount == 0:
            raise InsufficientData

        datapoints, avg_cpu = await cur.fetchone()
        required_datapoints = (period * 60 / SCRAPE_INTERVAL_SECONDS) * 0.5

        logger.debug('%s - required datapoints: %s got: %s', app_name, required_datapoints, datapoints)

        if datapoints > required_datapoints:
            return avg_cpu

        raise InsufficientData


async def is_new_app(app_name, space_name, conn):
    """Only attempt to scale apps that have existed for a few minutes at least.
    This stops the autoscaler from interfering with blue/green deployments"""

    stmt = "SELECT count(*) FROM metrics " \
           "WHERE app=%s AND space=%s AND " \
           "timestamp < NOW() - INTERVAL '10 MINUTE'" \
           "GROUP BY app;"

    async with conn.cursor() as cur:
        await cur.execute(stmt, (app_name, space_name,))

        return cur.rowcount == 0


@timer(PROM_GET_METRICS_TIME)
async def get_metrics(prometheus_exporter_url, username, password, conn):
    """Get app metrics from the prometheus exporter and store in database"""

    async def fetch(session, url):
        async with session.get(url) as response:
            return await response.text()

    async with aiohttp.ClientSession() as session:
        raw_metrics = await fetch(session, prometheus_exporter_url)

    stmt = 'INSERT INTO metrics (timestamp, space, app, instance_count, average_cpu) ' \
           'VALUES(now(), %s, %s, %s, %s);'

    metrics = group_cpu_metrics(get_cpu_metrics(raw_metrics))

    async with conn.cursor() as cur:
        for space, app, metric_values in metrics:
            await cur.execute(stmt, (
                app,
                space,
                len(metric_values),
                sum(metric_values) / len(metric_values)
            ))


@timer(PROM_AUTOSCALER_CHECK_TIME)
async def autoscale(conn):
    """The main autoscaling function"""
    loop = asyncio.get_event_loop()
    client = await loop.run_in_executor(None, get_client, CF_USERNAME, CF_PASSWORD)
    enabled_apps = await loop.run_in_executor(None, get_enabled_apps, client)

    insufficient_data_count = 0

    for app in enabled_apps:
        app_name = app['entity']['name']
        space = await loop.run_in_executor(None, app.space)
        space_name = space['entity']['name']
        params = get_autoscaling_params(app)
        notification = None
        desired_instance_count = params['instances']

        # to avoid issues with blue/green deployments we only want to manage apps
        # that have several minutes of data, indicating that it's not a transitory
        # app created as part of the deployment process
        if await is_new_app(app_name, space_name, conn):
            continue

        try:
            average_cpu = await get_avg_cpu(app_name, space_name, params['threshold_period'], conn)
        except InsufficientData:
            insufficient_data_count += 1
            continue

        if params['instances'] > params['max_instances']:
            notification = 'scaled down as instance count is above maximum'
            desired_instance_count = params['instances'] - 1

        elif params['instances'] < params['min_instances']:
            notification = 'scaled up as instance count is below minimum'
            desired_instance_count = params['instances'] + 1

        elif (average_cpu < params['low_threshold'] and
            params['instances'] > params['min_instances'] and
            not await is_cooldown(app_name, space_name, params['scale_down_delay'], conn)):

            desired_instance_count = params['instances'] - 1
            notification = f'scaled down to {desired_instance_count} - avg cpu {average_cpu}'

        elif (average_cpu > params['high_threshold'] and
            params['instances'] < params['max_instances'] and
            not await is_cooldown(app_name, space_name, params['scale_up_delay'], conn)):

            desired_instance_count = params['instances'] + 1
            notification = f'scaled up to {desired_instance_count} - avg cpu {average_cpu}'

        if notification:
            await notify(app_name, notification)

        if desired_instance_count != params['instances']:
            await scale(app, space_name, desired_instance_count, conn)
            PROM_SCALING_ACTIONS.inc({})

    PROM_INSUFFICIENT_DATA.set({}, insufficient_data_count)
    PROM_AUTOSCALING_ENABLED.set({}, len(enabled_apps))


async def periodic_run_autoscaler(pool):
    """Periodically scan apps, check autoscaling config and status"""

    while True:
        async with pool.acquire() as conn:
            logger.info('checking app autoscaling status')
            await autoscale(conn)
        await asyncio.sleep(APP_CHECK_INTERVAL_SECONDS)


async def periodic_get_metrics(pool):
    """Periodically scrape app metrics from the prometheus exporter"""

    while True:
        async with pool.acquire() as conn:
            logger.info('retrieving metrics')
            await get_metrics(PROM_PAAS_EXPORTER_URL,
                              PROM_PAAS_EXPORTER_USERNAME,
                              PROM_PAAS_EXPORTER_PASSWORD,
                              conn)
        await asyncio.sleep(SCRAPE_INTERVAL_SECONDS)


async def periodic_remove_old_data(pool):
    while True:
        logger.info('removing old data')
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM metrics where timestamp < now() - INTERVAL '20 min';")
        await asyncio.sleep(120)


async def start_webapp(port):
    prometheus_service = Service()
    prometheus_service.register(PROM_GET_METRICS_TIME)
    prometheus_service.register(PROM_AUTOSCALER_CHECK_TIME)
    prometheus_service.register(PROM_AUTOSCALING_ENABLED)
    prometheus_service.register(PROM_INSUFFICIENT_DATA)
    prometheus_service.register(PROM_SCALING_ACTIONS)

    app = web.Application()
    app.add_routes([web.get('/check', lambda _: web.Response(text='OK')),
                    web.get('/metrics', prometheus_service.handle_metrics)])

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()


async def main(sentry_client):
    loop = asyncio.get_event_loop()

    logger.info('starting web interface on %s', PORT)
    await start_webapp(PORT)

    try:
        async with aiopg.create_pool(DATABASE_URL) as pool:
            await asyncio.gather(
                loop.create_task(periodic_get_metrics(pool)),
                loop.create_task(periodic_run_autoscaler(pool)),
                loop.create_task(periodic_remove_old_data(pool)))

    except BaseException:
        sentry_client.captureException()
        await sentry_client.remote.get_transport().close()
        loop.stop()


if __name__ == '__main__':
    sentry_client = Client(SENTRY_DSN, transport=partial(QueuedAioHttpTransport, workers=5, qsize=1000))

    loop = asyncio.get_event_loop()
    loop.set_debug(DEBUG)
    loop.create_task(main(sentry_client))
    loop.run_forever()
