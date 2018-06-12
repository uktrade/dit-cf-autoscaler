import datetime as dt
import pytest

from autoscaler.app import get_autoscaling_params, is_cooldown, get_cpu_metrics
import autoscaler.app as autoscaler


@pytest.mark.parametrize("test_input,expected", [
    ({'AUTOSCALING': 'on'}, {'enabled': True, 'test': False}),
    ({'AUTOSCALING': 'off'}, {'enabled': False, 'test': False}),
    ({'AUTOSCALING': 'test'}, {'enabled': True, 'test': True}),
    ({}, {'enabled': False, 'test': False}),
])
def test_get_autoscaling_params_autoscaling(app_factory, test_input, expected):

    app = app_factory('my_app', **test_input)
    params = get_autoscaling_params(app)

    assert params['enabled'] == expected['enabled']
    assert params['test'] == expected['test']


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

    assert params['threshold_period'] == autoscaler.DEFAULT_THRESHOLD_PERIOD
    assert params['high_threshold'] == autoscaler.DEFAULT_LOW_THRESHOLD
    assert params['low_threshold'] == autoscaler.DEFAULT_LOW_THRESHOLD
    assert params['cooldown'] == autoscaler.DEFAULT_COOLDOWN_PERIOD


@pytest.mark.asyncio
async def test_is_cooldown_true(conn, create_action):
    await create_action(dt.datetime.now() - dt.timedelta(minute=-6), 'test_app', 'test_space', 1)

    assert await is_cooldown('test_app', 'test_space', 5, conn)


@pytest.mark.asyncio
async def test_is_cooldown_true(conn, create_action):
    await create_action(dt.datetime.now(), 'test_app', 'test_space', 1)

    assert await is_cooldown('test_app', 'test_space', 5, conn)


def test_get_cpu_metrics(prom_exporter_text):
    results = [item for item in get_cpu_metrics(prom_exporter_text)]

    expected = [
        ('cpu', 'activity-stream', 0, 'activity-stream', 0.0),
        ('cpu', 'activity-stream', 0, 'activity-stream', 0.0),
        ('cpu', 'cert-monitor-production', 0, 'webops', 0.0),
        ('cpu', 'cert-monitor-production', 0, 'webops', 0.0),
        ('cpu', 'contact-ukti', 0, 'exopps', 0.0), ('cpu', 'contact-ukti', 1, 'exopps', 0.0),
        ('cpu', 'datahub', 0, 'datahub', 0.0), ('cpu', 'datahub', 3, 'datahub', 1.0)]

    assert results == expected

