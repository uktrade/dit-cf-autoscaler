from pathlib import Path

import pytest
from async_generator import yield_, async_generator

import aiopg
from autoscaler import app


@pytest.fixture
def app_factory():
    def _make_app(app_name='test_app', instances=1, **env_vars):

        app = {
            'entity': {
                'name': app_name,
                'instances': instances,
                'environment_json': {
                }
            }
        }

        app['entity']['environment_json'] = env_vars

        return app

    return _make_app


@pytest.fixture
@async_generator
async def pool():
    async with aiopg.create_pool(app.DATABASE_URL) as pool:
        await yield_(pool)


@pytest.fixture
@async_generator
async def conn(pool):
    async with pool.acquire() as conn:
        await yield_(conn)


@pytest.fixture
async def create_action(conn):
    async def _create(timestamp, app, space, instances):
        async with conn.cursor() as cur:
            await cur.execute(
                'INSERT INTO actions (timestamp, app, space, instances) VALUES(%s, %s, %s, %s)',
                (timestamp, app, space, instances,))

    return _create


@pytest.fixture
async def create_metric(conn):
    async def _create(timestamp, app, space, instance, value):
        async with conn.cursor() as cur:
            await cur.execute(
                'INSERT INTO metrics (timestamp, metric, app, space, instance, value) '
                'VALUES(%s, \'cpu\', %s, %s, %s, %s)',
                (timestamp, app, space, instance, value))

    return _create


@pytest.fixture
def prom_exporter_text():
    path = Path('test/fixtures/prometheus-exporter.txt')

    return open(path).read()
