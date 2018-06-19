from unittest.mock import MagicMock
from pathlib import Path

import pytest
from async_generator import yield_, async_generator

import aiohttp
import aiopg
from autoscaler import app


class MockCfApp:
    client = None
    apps = None
    space = None

    def __init__(self, app_data, space_data):
        self._data = app_data

        self.client = MagicMock()
        self.apps = MagicMock()
        self.space = MagicMock(return_value=space_data)

    def __getitem__(self, key):
        return self._data[key]

    def __repr__(self):
        return str(self._data)


@pytest.fixture
def app_factory():
    def _make_app(app_name='test_app', space_name='test_space', instances=1, **env_vars):

        app_data = {
            'entity': {
                'name': app_name,
                'instances': instances,
                'environment_json': {
                }
            },
            'metadata': {
                'guid': 'C56A4180-65AA-42EC-A945-5FD21DEC0538'
            }
        }

        space_data = {
            'entity': {
                'name': space_name
            }
        }

        app_data['entity']['environment_json'] = env_vars

        return MockCfApp(app_data, space_data)

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
    async def _create(timestamp, app, space, instance_count, average_cpu):
        async with conn.cursor() as cur:
            await cur.execute(
                'INSERT INTO metrics (timestamp, app, space, instance_count, average_cpu) '
                'VALUES(%s, %s, %s, %s, %s)',
                (timestamp, app, space, instance_count, average_cpu))

    return _create


@pytest.fixture
def prom_exporter_text():
    path = Path('test/fixtures/prometheus-exporter.txt')

    return open(path).read()


@pytest.fixture
@async_generator
async def test_http_client():
    async with aiohttp.ClientSession() as session:
        await yield_(session)
