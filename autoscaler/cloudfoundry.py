from cloudfoundry_client.client import CloudFoundryClient


def get_client(username, password, http_proxy='', https_proxy=''):
    target_endpoint = 'https://api.cloud.service.gov.uk'
    proxy = dict(http=http_proxy, https=https_proxy)
    client = CloudFoundryClient(target_endpoint, proxy=proxy)
    client.init_with_user_credentials(username, password)

    return client
