import os

from cloudfoundry_client.client import CloudFoundryClient


def get_client(username, password):
    target_endpoint = 'https://api.cloud.service.gov.uk'
    proxy = dict(http=os.environ.get('HTTP_PROXY', ''), https=os.environ.get('HTTPS_PROXY', ''))
    client = CloudFoundryClient(target_endpoint, proxy=proxy, skip_verification=True)
    client.init_with_user_credentials(username, password)

    return client


def get_app(space, app, client):
    """Return a cloudfoundry client"""
    # TODO: improve exceptions
    for current_space in client.spaces:
        if current_space['entity']['name'] == space:
            for current_app in current_space.apps():
                if current_app['entity']['name'] == app:
                    return current_app
            else:
                raise Exception('App not found')
    else:
        raise Exception('Space not found')
