import datetime as dt
import pytest

from autoscaler.app import get_autoscaling_params, is_cooldown, get_cpu_metrics
import autoscaler.app as autoscaler


async def reset_database(conn):
    async with conn.cursor() as cur:
        await cur.execute('DELETE FROM actions;')
        await cur.execute('DELETE FROM metrics;')


@pytest.mark.parametrize("test_input,expected", [
    ({'AUTOSCALING': 'on'}, True),
    ({'AUTOSCALING': 'off'}, False),
    ({}, False),
])
def test_get_autoscaling_params_autoscaling(app_factory, test_input, expected):

    app = app_factory('my_app', **test_input)
    params = get_autoscaling_params(app)

    assert params['enabled'] == expected


@pytest.mark.parametrize("test_input,expected", [
    ({'AUTOSCALING_MIN': 1, 'AUTOSCALING_MAX': 20},
     {'min': 1, 'max': 20}),
    ({}, {'min': autoscaler.DEFAULT_MINIMUM_INSTANCES,
          'max': autoscaler.DEFAULT_MAXIMUM_INSTANCES}),
])
def test_get_autoscaling_params_autoscaling_min_max(app_factory, test_input, expected):
    app = app_factory('my_app', **test_input)
    params = get_autoscaling_params(app)

    assert params['min_instances'] == expected['min']
    assert params['max_instances'] == expected['max']


def test_test_get_autoscaling_params(app_factory):
    app = app_factory('my_app')
    params = get_autoscaling_params(app)

    assert params['threshold_period'] == autoscaler.DEFAULT_THRESHOLD_PERIOD_MINUTES
    assert params['high_threshold'] == autoscaler.DEFAULT_HIGH_THRESHOLD_CPU_PERCENTAGE
    assert params['low_threshold'] == autoscaler.DEFAULT_LOW_THRESHOLD_CPU_PERCENTAGE
    assert params['cooldown'] == autoscaler.DEFAULT_COOLDOWN_PERIOD_MINUTES


@pytest.mark.asyncio
async def test_is_cooldown_false(conn, create_action):
    await create_action(dt.datetime.utcnow() - dt.timedelta(minutes=6), 'test_app', 'test_space', 1)

    assert not await is_cooldown('test_app', 'test_space', 5, conn)


@pytest.mark.asyncio
async def test_is_cooldown_false(conn, create_action):
    await reset_database(conn)
    await create_action(dt.datetime.now(), 'test_app', 'test_space', 1)

    assert await is_cooldown('test_app', 'test_space', 5, conn)


def test_get_cpu_metrics(prom_exporter_text):
    results = [item for item in get_cpu_metrics(prom_exporter_text)]

    expected = [{'metric': 'cpu', 'app': 'activity-stream', 'instance': 0, 'space': 'activity-stream', 'value': 0.0},
     {'metric': 'cpu', 'app': 'activity-stream', 'instance': 0, 'space': 'activity-stream', 'value': 0.0},
     {'metric': 'cpu', 'app': 'cert-monitor-production', 'instance': 0, 'space': 'webops', 'value': 0.0},
     {'metric': 'cpu', 'app': 'cert-monitor-production', 'instance': 0, 'space': 'webops', 'value': 0.0},
     {'metric': 'cpu', 'app': 'contact-ukti', 'instance': 0, 'space': 'exopps', 'value': 0.0},
     {'metric': 'cpu', 'app': 'contact-ukti', 'instance': 1, 'space': 'exopps', 'value': 0.0},
     {'metric': 'cpu', 'app': 'datahub', 'instance': 0, 'space': 'datahub', 'value': 0.0},
     {'metric': 'cpu', 'app': 'datahub', 'instance': 3, 'space': 'datahub', 'value': 1.0}]

    assert results == expected
