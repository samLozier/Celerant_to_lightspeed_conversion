import json

import requests

import config

# URL use to authenticate user: https://cloud.lightspeedapp.com/oauth/authorize.php?response_type=code&client_id={client_id}&scope=employee:all

url = 'https://cloud.lightspeedapp.com/oauth/access_token.php'

clientID = config.lightspeed['clientID']
client_secret = config.lightspeed['clientSec']
code = config.lightspeed['code']

# fill in payload here
payload = {
    'client_id': clientID,
    'client_secret': client_secret,
    'code': code,
    "grant_type": 'authorization_code'
}

headers = {'content-type': 'application/json'}

r = requests.request('POST', url, data=json.dumps(payload), headers=headers)

# Logs credentials to the virtual console
print(r.json())
data = r.json()

# creates credentials file with json response
with open('credentials.json', 'a') as outfile:
    json.dump(data, outfile, indent=4)
