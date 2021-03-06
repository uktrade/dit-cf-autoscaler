import datetime as dt
import pytest

from asynctest import patch as async_patch

from autoscaler.app import get_autoscaling_params, is_cooldown, get_cpu_metrics, get_metrics, \
    start_webapp, scale, InsufficientData, get_avg_cpu, get_enabled_apps, autoscale

from autoscaler import app as main


async def reset_database(conn):
    async with conn.cursor() as cur:
        await cur.execute('DELETE FROM actions;')
        await cur.execute('DELETE FROM metrics;')


@pytest.mark.parametrize("test_input,expected", [
    ({'X_AUTOSCALING': 'on'}, True),
    ({'X_AUTOSCALING': 'off'}, False),
    ({}, False),
])
def test_get_autoscaling_params_autoscaling(app_factory, test_input, expected):

    app = app_factory('my_app', **test_input)
    params = get_autoscaling_params(app)

    assert params['enabled'] == expected


@pytest.mark.parametrize("test_input,expected", [
    ({'X_AUTOSCALING_MIN': 1, 'X_AUTOSCALING_MAX': 20},
     {'min': 1, 'max': 20}),
    ({}, {'min': main.DEFAULT_MINIMUM_INSTANCES,
          'max': main.DEFAULT_MAXIMUM_INSTANCES}),
])
def test_get_autoscaling_params_autoscaling_min_max(app_factory, test_input, expected):
    app = app_factory('my_app', **test_input)
    params = get_autoscaling_params(app)

    assert params['min_instances'] == expected['min']
    assert params['max_instances'] == expected['max']


@pytest.mark.parametrize("test_input,expected", [
    ({'X_AUTOSCALING_CPU_LOW_THRESHOLD': 25, 'X_AUTOSCALING_CPU_HIGH_THRESHOLD': 300},
     {'low': 25, 'high': 300}),
    ({}, {'low': main.DEFAULT_LOW_THRESHOLD_CPU_PERCENTAGE,
          'high': main.DEFAULT_HIGH_THRESHOLD_CPU_PERCENTAGE}),
])
def test_get_autoscaling_params_autoscaling_cpu_threshold(app_factory, test_input, expected):
    app = app_factory('my_app', **test_input)
    params = get_autoscaling_params(app)

    assert params['low_threshold'] == expected['low']
    assert params['high_threshold'] == expected['high']


def test_test_get_autoscaling_params(app_factory):
    app = app_factory('my_app')
    params = get_autoscaling_params(app)

    assert params['threshold_period'] == main.DEFAULT_THRESHOLD_PERIOD_MINUTES
    assert params['high_threshold'] == main.DEFAULT_HIGH_THRESHOLD_CPU_PERCENTAGE
    assert params['low_threshold'] == main.DEFAULT_LOW_THRESHOLD_CPU_PERCENTAGE
    assert params['scale_up_delay'] == main.DEFAULT_SCALE_UP_DELAY_MINUTES
    assert params['scale_down_delay'] == main.DEFAULT_SCALE_DOWN_DELAY_MINUTES


@pytest.mark.asyncio
async def test_is_cooldown_false(conn, create_action):
    await reset_database(conn)

    await create_action(dt.datetime.utcnow() - dt.timedelta(minutes=6), 'test_space', 'test_app', 1)

    assert not await is_cooldown('test_app', 'test_space', 5, conn)


@pytest.mark.asyncio
async def test_is_cooldown_true(conn, create_action):
    await reset_database(conn)
    await create_action(dt.datetime.now(), 'test_app', 'test_space', 1)

    assert await is_cooldown('test_app', 'test_space', 5, conn)


def test_get_cpu_metrics(prom_exporter_text):
    results = [item for item in get_cpu_metrics(prom_exporter_text)]

    expected = [
        {'app': 'activity-stream', 'instance': 0, 'space': 'activity-stream', 'value': 0.0},
        {'app': 'activity-stream', 'instance': 0, 'space': 'activity-stream', 'value': 0.0},
        {'app': 'cert-monitor-production', 'instance': 0, 'space': 'webops', 'value': 0.0},
        {'app': 'cert-monitor-production', 'instance': 0, 'space': 'webops', 'value': 0.0},
        {'app': 'contact-ukti', 'instance': 0, 'space': 'exopps', 'value': 0.0},
        {'app': 'contact-ukti', 'instance': 1, 'space': 'exopps', 'value': 0.0},
        {'app': 'datahub', 'instance': 0, 'space': 'datahub', 'value': 0.0},
        {'app': 'datahub', 'instance': 3, 'space': 'datahub', 'value': 1.0}]

    assert results == expected


@pytest.mark.asyncio
async def test_get_metrics(httpserver, prom_exporter_text, conn):
    httpserver.serve_content(prom_exporter_text)

    await reset_database(conn)
    await get_metrics(httpserver.url, 'test', 'test', conn)

    async with conn.cursor()as cur:
        await cur.execute('SELECT COUNT(*) FROM metrics;')
        result = await cur.fetchone()
        assert result[0] == 4

        await cur.execute('SELECT app, space, instance_count, average_cpu FROM metrics ORDER BY app;')

        results = []
        async for row in cur:
            results.append(row)

        expected = [
            ('activity-stream', 'activity-stream', 2, 0.0),
            ('cert-monitor-production', 'webops', 2, 0.0),
            ('contact-ukti', 'exopps', 2, 0.0),
            ('datahub', 'datahub', 2, 0.5)]

        assert results == expected


@pytest.mark.asyncio
async def test_scale(conn, app_factory):

    mock_app = app_factory('test_app')

    await reset_database(conn)
    await scale(mock_app, 'test_space', 5, conn)

    async with conn.cursor() as cur:
        await cur.execute('SELECT app, space, instances FROM actions;')

        assert cur.rowcount == 1
        assert await cur.fetchone() == ('test_app', 'test_space', 5)
        assert mock_app.client.apps._update.called_with(mock_app['metadata']['guid'], 5)


@pytest.mark.asyncio
async def test_start_webapp(test_http_client):

    port = 3333
    await start_webapp(port)

    resp = await test_http_client.get(f'http://localhost:{port}/metrics')
    assert resp.status == 200

    resp = await test_http_client.get(f'http://localhost:{port}/check')
    assert resp.status == 200


@pytest.mark.asyncio
async def test_get_avg_cpu(create_metric, conn):
    await reset_database(conn)

    current_timestamp = dt.datetime.now() - dt.timedelta(minutes=5)

    for i in range(35):
        await create_metric(current_timestamp, 'test_app', 'test_space', 1, 10)
        current_timestamp += dt.timedelta(seconds=1)

    average_cpu = await get_avg_cpu('test_app', 'test_space', 5, conn)

    assert average_cpu == 10


@pytest.mark.asyncio
async def test_get_avg_cpu_insufficient_data(create_metric, conn):
    await reset_database(conn)

    await create_metric(dt.datetime.now() - dt.timedelta(minutes=1), 'test_app', ' test_space', 1, 10)

    with pytest.raises(InsufficientData):
        await get_avg_cpu('test_app', 'test_space', 5, conn)


def test_get_enabled_apps(mocker, app_factory):
    apps = [
        app_factory('test_app1', 'test_space1', X_AUTOSCALING='False'),
        app_factory('test_app2', 'test_space2', X_AUTOSCALING='on'),
        app_factory('test_app3', 'test_space3')
    ]

    mock_client = mocker.MagicMock()
    mock_client.apps.list.return_value = apps

    enabled_apps = get_enabled_apps(mock_client)

    assert len(enabled_apps) == 1
    assert enabled_apps[0] == apps[1]


@pytest.mark.asyncio
async def test_autoscale_is_cooldown(mocker, app_factory, conn, create_action):
    apps = [
        app_factory('test_app', 'test_space', X_AUTOSCALING='on'),
    ]

    await create_action(dt.datetime.now(), 'test_app', 'test_space', 1)

    mock_get_client = mocker.patch('autoscaler.app.get_client')
    mock_get_client.return_value.apps.list.return_value = apps
    with async_patch('autoscaler.app.notify') as mock_notify:
        await autoscale(conn)

    assert mock_notify.called_with('is in cool down period', is_verbose=True)


@pytest.mark.asyncio
async def test_autoscale_insufficient_data(mocker, app_factory, conn):
    await reset_database(conn)
    apps = [
        app_factory('test_app', 'test_space', X_AUTOSCALING='on', X_AUTOSCALING_MIN=1),
    ]

    mock_get_client = mocker.patch('autoscaler.app.get_client')
    mock_get_client.return_value.apps.list.return_value = apps
    with async_patch('autoscaler.app.notify') as mock_notify:
        with async_patch('autoscaler.app.scale') as mock_scale:
            await autoscale(conn)

    assert not mock_scale.called

@pytest.mark.asyncio
async def test_autoscale_at_min_scale(mocker, app_factory, conn, create_metric):
    await reset_database(conn)

    apps = [
        app_factory('test_app', 'test_space', instances=2, X_AUTOSCALING='on', X_AUTOSCALING_MIN=2),
    ]

    for i in range(35):
        await create_metric(dt.datetime.now()-dt.timedelta(seconds=30), 'test_app', 'test_space', 2, 1)

    mock_get_client = mocker.patch('autoscaler.app.get_client')
    mock_get_client.return_value.apps.list.return_value = apps
    with async_patch('autoscaler.app.notify') as mock_notify:
        with async_patch('autoscaler.app.scale') as mock_scale:
            await autoscale(conn)

    assert not mock_notify.called
    assert not mock_scale.called


@pytest.mark.asyncio
async def test_autoscale_at_max_scale(mocker, app_factory, conn, create_metric):
    await reset_database(conn)

    apps = [
        app_factory('test_app', 'test_space', instances=10, X_AUTOSCALING='on', X_AUTOSCALING_MAX=10),
    ]

    for i in range(10):
        await create_metric(dt.datetime.now()-dt.timedelta(seconds=30), 'test_app', 'test_space', 2, 95)

    mock_get_client = mocker.patch('autoscaler.app.get_client')
    mock_get_client.return_value.apps.list.return_value = apps
    with async_patch('autoscaler.app.notify') as mock_notify:
        with async_patch('autoscaler.app.scale') as mock_scale:
            await autoscale(conn)

    assert not mock_notify.called
    assert not mock_scale.called


@pytest.mark.asyncio
async def test_autoscale_scale_up(mocker, app_factory, conn, create_metric):

    await reset_database(conn)

    apps = [
        app_factory('test_app', 'test_space', instances=5, X_AUTOSCALING='on', X_AUTOSCALING_MAX=10),
    ]

    # pass is_new_app test - this ensures that we don't attempt to autoscale transitory apps created
    # as part of blue/green depoloyment process.
    await create_metric(dt.datetime.now() - dt.timedelta(minutes=31), 'test_app', 'test_space', 2, 95)

    for i in range(35):
        await create_metric(dt.datetime.now() - dt.timedelta(seconds=30), 'test_app', 'test_space', 2, 95)

    mock_get_client = mocker.patch('autoscaler.app.get_client')
    mock_get_client.return_value.apps.list.return_value = apps
    with async_patch('autoscaler.app.notify') as mock_notify:
        await autoscale(conn)

    async with conn.cursor() as cur:
        await cur.execute('SELECT app, space, instances FROM actions;')

        assert cur.rowcount == 1
        assert await cur.fetchone() == ('test_app', 'test_space', 6)

    assert mock_notify.called
    assert mock_notify.call_args[0] == ('test_app', 'scaled up to 6 - avg cpu 95.00')


@pytest.mark.asyncio
async def test_autoscale_scale_up_indicates_it_is_at_max(mocker, app_factory, conn, create_metric):

    await reset_database(conn)

    apps = [
        app_factory('test_app', 'test_space', instances=9, X_AUTOSCALING='on', X_AUTOSCALING_MAX=10),
    ]

    # pass is_new_app test - this ensures that we don't attempt to autoscale transitory apps created
    # as part of blue/green depoloyment process.
    await create_metric(dt.datetime.now() - dt.timedelta(minutes=31), 'test_app', 'test_space', 2, 95)

    for i in range(35):
        await create_metric(dt.datetime.now() - dt.timedelta(seconds=30), 'test_app', 'test_space', 2, 95)

    mock_get_client = mocker.patch('autoscaler.app.get_client')
    mock_get_client.return_value.apps.list.return_value = apps
    with async_patch('autoscaler.app.notify') as mock_notify:
        await autoscale(conn)

    assert mock_notify.called
    assert mock_notify.call_args[0] == ('test_app', 'scaled up to 10 - avg cpu 95.00 [max]')


@pytest.mark.asyncio
async def test_autoscale_scale_down(mocker, app_factory, conn, create_metric):

    await reset_database(conn)

    apps = [
        app_factory('test_app', 'test_space', instances=5, X_AUTOSCALING='on', X_AUTOSCALING_MIN=2),
    ]

    await create_metric(dt.datetime.now() - dt.timedelta(minutes=30), 'test_app', 'test_space', 5, 5)

    for i in range(10):
        await create_metric(dt.datetime.now() - dt.timedelta(seconds=30), 'test_app', 'test_space', 5, 5)

    mock_get_client = mocker.patch('autoscaler.app.get_client')
    mock_get_client.return_value.apps.list.return_value = apps
    with async_patch('autoscaler.app.notify') as mock_notify:
        await autoscale(conn)

    async with conn.cursor() as cur:
        await cur.execute('SELECT app, space, instances FROM actions;')

        assert cur.rowcount == 1
        assert await cur.fetchone() == ('test_app', 'test_space', 4)

    assert mock_notify.called
    assert mock_notify.call_args[0] == ('test_app', 'scaled down to 4 - avg cpu 5.00')


@pytest.mark.asyncio
async def test_autoscale_scale_down_to_min_indicates_it_is_now_at_min(mocker, app_factory, conn, create_metric):

    await reset_database(conn)

    apps = [
        app_factory('test_app', 'test_space', instances=3, X_AUTOSCALING='on', X_AUTOSCALING_MIN=2),
    ]

    await create_metric(dt.datetime.now() - dt.timedelta(minutes=30), 'test_app', 'test_space', 5, 5)

    for i in range(10):
        await create_metric(dt.datetime.now() - dt.timedelta(seconds=30), 'test_app', 'test_space', 5, 5)

    mock_get_client = mocker.patch('autoscaler.app.get_client')
    mock_get_client.return_value.apps.list.return_value = apps
    with async_patch('autoscaler.app.notify') as mock_notify:
        await autoscale(conn)

    assert mock_notify.called
    assert mock_notify.call_args[0] == ('test_app', 'scaled down to 2 - avg cpu 5.00 [min]')


@pytest.mark.asyncio
async def test_autoscale_scales_up_if_below_min(mocker, app_factory, create_metric, conn):

    await reset_database(conn)

    apps = [
        app_factory('test_app', 'test_space', instances=1, X_AUTOSCALING='on', X_AUTOSCALING_MIN=2),
    ]

    await create_metric(dt.datetime.now() - dt.timedelta(minutes=30), 'test_app', 'test_space', 5, 5)

    for i in range(10):
        await create_metric(dt.datetime.now()-dt.timedelta(seconds=30), 'test_app', 'test_space', 2, 95)

    mock_get_client = mocker.patch('autoscaler.app.get_client')
    mock_get_client.return_value.apps.list.return_value = apps
    with async_patch('autoscaler.app.notify') as mock_notify:
        await autoscale(conn)

    assert mock_notify.called
    assert mock_notify.call_args[0] == ('test_app', 'scaled up to 2 as instance count below minimum')


@pytest.mark.asyncio
async def test_autoscale_scales_down_if_above_max(mocker, app_factory, create_metric, conn):

    await reset_database(conn)

    apps = [
        app_factory('test_app', 'test_space', instances=11, X_AUTOSCALING='on', X_AUTOSCALING_MAX=10),
    ]

    await create_metric(dt.datetime.now() - dt.timedelta(minutes=30), 'test_app', 'test_space', 5, 5)

    for i in range(10):
        await create_metric(dt.datetime.now()-dt.timedelta(seconds=30), 'test_app', 'test_space', 2, 95)

    mock_get_client = mocker.patch('autoscaler.app.get_client')
    mock_get_client.return_value.apps.list.return_value = apps
    with async_patch('autoscaler.app.notify') as mock_notify:
        await autoscale(conn)

    assert mock_notify.called
    assert mock_notify.call_args[0] == ('test_app', 'scaled down to 10 as instance count above maximum')
