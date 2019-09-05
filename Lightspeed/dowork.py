import json

import data_connections.developer_credentials as developer_credentials
from data_connections.lightspeed_conn import LightspeedStoreConn, LightspeedReports

with open('data_connections/store_credentials.json') as json_file:
    store_credentials_data = json.load(json_file)

devcreds = developer_credentials.lightspeed

conn = LightspeedStoreConn(store_credentials_data, devcreds)

conn.refresh_access()

print(conn.expires_in)

orders = LightspeedReports.get_orders(conn)

with open('output_test.json', 'w+') as outfile:
    json.dump(orders, outfile)
