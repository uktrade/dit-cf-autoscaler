import os

from cloudfoundry_client.client import CloudFoundryClient


def get_client(username, password):
    target_endpoint = 'https://api.cloud.service.gov.uk'
    proxy = dict(http=os.environ.get('HTTP_PROXY', ''), https=os.environ.get('HTTPS_PROXY', ''))
    client = CloudFoundryClient(target_endpoint, proxy=proxy, skip_verification=True)
    client.init_with_user_credentials(username, password)

    return client
