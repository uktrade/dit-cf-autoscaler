#!/usr/bin/env python
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
from raven_aiohttp import AioHttpTransport

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
DEFAULT_THRESHOLD_PERIOD_MINUTES = int(os.getenv('DEFAULT_THRESHOLD_PERIOD_MINUTES', 5))
DEFAULT_MINIMUM_INSTANCES = int(os.getenv('DEFAULT_MINIMUM_INSTANCES', 2))
DEFAULT_MAXIMUM_INSTANCES = int(os.getenv('DEFAULT_MAXIMUM_INSTANCES', 10))
DEFAULT_COOLDOWN_PERIOD_MINUTES = int(os.getenv('DEFAULT_COOLDOWN_PERIOD_MINUTES', 5))
ENABLE_VERBOSE_OUTPUT = os.getenv('ENABLE_VERBOSE_OUTPUT', 'False') in TRUTHY_VALUES
SENTRY_DSN = os.getenv('SENTRY_DSN')

# prometheus metrics:
PROM_GET_METRICS_TIME = Summary('get_metrics', 'the time it takes to get application metrics')
PROM_AUTOSCALER_CHECK_TIME = Summary('autoscaler_check_time', 'the time it takes for the autoscaler check to complete')
PROM_SCALING_ACTIONS = Counter('scaling_actions', 'a count of the number of scaling actions that have taken place')
PROM_AUTOSCALING_ENABLED = Gauge('autoscaling_enabled', 'number of apps that have autoscaling enabled')
PROM_INSUFFICIENT_DATA = Gauge('insufficient_data', 'number of apps that have insufficient data')
PROM_APPS_AT_MIN_SCALE = Gauge('min_scale', 'apps that cannot scale down due to being at min instances')
PROM_APPS_AT_MAX_SCALE = Gauge('max_scale', 'apps that cannot scale up due to being at max instances')
PROM_APP_METRICS = Gauge('app_metrics', 'the number of apps that are supplying metrics')


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


@timer(PROM_GET_METRICS_TIME)
async def get_metrics(prometheus_exporter_url, username, password, conn):
    """Get app metrics from the prometheus exporter and store in database"""

    if username:
        auth = aiohttp.helpers.BasicAuth(username, password, )
    else:
        auth = aiohttp.helpers.BasicAuth()

    async def fetch(session, url):
        async with session.get(url) as response:
            return await response.text()

    async with aiohttp.ClientSession(auth=auth) as session:
        raw_metrics = await fetch(session, prometheus_exporter_url)

    stmt = 'INSERT INTO metrics (timestamp, space, app, instance_count, average_cpu) ' \
           'VALUES(now(), %s, %s, %s, %s);'

    metrics = group_cpu_metrics(get_cpu_metrics(raw_metrics))

    PROM_APP_METRICS.set({}, len(metrics))

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

    counts = {
        'at_min_scale': 0,
        'at_max_scale': 0,
        'insufficient_data': 0
    }

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
            counts['insufficient_data'] += 1
            await notify(app_name, 'insufficient data', is_verbose=True)
            continue

        if params['low_threshold'] <= average_cpu <= params['high_threshold']:
            await notify(app_name, f'is within bounds - current: {average_cpu}', is_verbose=True)

        elif average_cpu > params['high_threshold']:
            if params['instances'] >= params['max_instances']:
                counts['at_max_scale'] += 1
                await notify(app_name, 'cannot scale up - already at max', is_verbose=True)
            else:
                new_instance_count = params['instances'] + 1
                await scale(app, space_name, new_instance_count, conn)
                PROM_SCALING_ACTIONS.inc({})

                await notify(app_name, f'scaled up to {new_instance_count} - avg cpu {average_cpu}')

        elif average_cpu < params['low_threshold']:
            if params['instances'] <= params['min_instances']:
                counts['at_min_scale'] += 1
                await notify(app_name, 'cannot scale down - already at min', is_verbose=True)
            else:
                new_instance_count = params['instances'] - 1
                await scale(app, space_name, new_instance_count, conn)
                PROM_SCALING_ACTIONS.inc({})

                await notify(app_name, f'scaled down to {new_instance_count} - avg cpu {average_cpu}')

    PROM_INSUFFICIENT_DATA.set({}, counts['insufficient_data'])
    PROM_APPS_AT_MIN_SCALE.set({}, counts['at_min_scale'])
    PROM_APPS_AT_MAX_SCALE.set({}, counts['at_max_scale'])
    PROM_AUTOSCALING_ENABLED.set({}, len(enabled_apps))

    return counts


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
        PROM_PAAS_EXPORTER_USERNAME


async def periodic_remove_old_data(pool):
    while True:
        logger.info('removing old data')
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                cur.execute("DELETE FROM metrics where timestamp < now() - INTERVAL '20 min';")
        await asyncio.sleep(300)


async def start_webapp(port):
    prometheus_service = Service()
    prometheus_service.register(PROM_GET_METRICS_TIME)
    prometheus_service.register(PROM_AUTOSCALER_CHECK_TIME)
    prometheus_service.register(PROM_AUTOSCALING_ENABLED)
    prometheus_service.register(PROM_INSUFFICIENT_DATA)
    prometheus_service.register(PROM_SCALING_ACTIONS)
    prometheus_service.register(PROM_APPS_AT_MIN_SCALE)
    prometheus_service.register(PROM_APPS_AT_MAX_SCALE)
    prometheus_service.register(PROM_APP_METRICS)

    app = web.Application()
    app.add_routes([web.get('/check', lambda _: web.Response(text='OK')),
                    web.get('/metrics', prometheus_service.handle_metrics)])

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()


async def main():
    loop = asyncio.get_event_loop()

    await notify('general', 'starting autoscaler', is_verbose=True)

    logger.info('starting web interface on %s', PORT)
    await start_webapp(PORT)

    async with aiopg.create_pool(DATABASE_URL) as pool:
        await asyncio.gather(
            loop.create_task(periodic_get_metrics(pool)),
            loop.create_task(periodic_run_autoscaler(pool)),
            loop.create_task(periodic_remove_old_data(pool)))


def custom_exception_handler(loop, context):
    loop.default_exception_handler(context)

    print(context)
    loop.stop()


if __name__ == '__main__':
    if SENTRY_DSN:
        sentry_client = Client(SENTRY_DSN, ransport=AioHttpTransport)

    loop = asyncio.get_event_loop()
    loop.set_debug(DEBUG)
    loop.set_exception_handler(custom_exception_handler)
    loop.create_task(main())
    loop.run_forever()
