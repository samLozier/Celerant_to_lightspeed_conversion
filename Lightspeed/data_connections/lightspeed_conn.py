'''Establish a set of classes to facilitate reporting with Lightspeed'''
import requests


# TODO - need a way to manage rate limiting per connection

class LightspeedStoreConn:
    # TODO better credential storage?
    def __init__(self, store_credentials_data, devcreds):
        self.access_token = ''  # need to get a new one each time using the refresh method
        self.expires_in = ''  # This can update multiple times per session
        self.token_type = store_credentials_data['token_type']
        self.scope = store_credentials_data['scope']
        self.refresh_token = store_credentials_data['refresh_token']
        self.account_id = store_credentials_data['account_id']
        self.client_id = devcreds['clientID']
        self.client_secret = devcreds['clientSec']
        self.base_url = 'https://api.lightspeedapp.com/API/Account/'

    def refresh_access(self):
        payload = {
            'refresh_token': f'{self.refresh_token}',
            'client_secret': f'{self.client_secret}',
            'client_id': f'{self.client_id}',
            'grant_type': 'refresh_token',
        }
        r = requests.request("POST",
                             'https://cloud.lightspeedapp.com/oauth/access_token.php',
                             data=payload).json()
        self.access_token = r['access_token']
        self.expires_in = r['expires_in']


class LightspeedReports(LightspeedStoreConn):
    def get_categories(self):
        headers = {
            'authorization': f'Bearer {self.access_token}'
        }
        url = f'{self.base_url}{self.account_id}/Category.json'
        response = requests.request('GET', url, headers=headers).json()
        return response
        # TODO actually do something with this response

    def get_items(self, *kwarg):
        if kwarg is int:
            endpoint = f'/Item/{kwarg}.json'
        else:
            endpoint = f'/Item.json'

        headers = {
            'authorization': f'Bearer {self.access_token}'
        }
        url = f'{self.base_url}{self.account_id}{endpoint}'
        response = requests.request('GET', url, headers=headers).json()
        return response
        # TODO handle pagination

    def get_orders(self, *kwarg):
        if kwarg is int:
            endpoint = f'/Order/{kwarg}.json'
        else:
            endpoint = f'/Order.json'

        headers = {
            'authorization': f'Bearer {self.access_token}'
        }
        url = f'{self.base_url}{self.account_id}{endpoint}'
        response = requests.request('GET', url, headers=headers).json()
        return response
        # #TODO handle pagination

    def get_orders_custom_fields(self, *kwarg):
        headers = {
            'authorization': f'Bearer {self.access_token}'
        }
        endpoint = '/Order/CustomField.json'
        url = f'{self.base_url}{self.account_id}{endpoint}'
        response = requests.request('GET', url, headers=headers).json()
        return response
        # #TODO handle pagination
