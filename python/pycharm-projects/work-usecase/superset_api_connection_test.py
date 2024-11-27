import pandas as pd
import json
import requests
from IPython.display import JSON

#SET YOUR PARAMETERS HERE #
base_url = 'https://superset-sit.coexservices.com.au/'
#'http://localhost:8088/'
username = 'admin'
password = 'xxxxxxxx'
api_suffix = 'api/v1/log/'

#WILL GENERATE THE PAYLOAD THAT WILL THEN GIVE YOU AN ACCESS TOKEN
payload = {
    'username': username,
    'password': password,
    'provider': 'db'
}

r = requests.post(base_url + 'api/v1/security/login',json=payload)
#ACCESS TOKEN FOR THE USER

access_token = r.json()
print(access_token)

headersAuth = {
    'Authorization': 'Bearer ' + access_token['access_token']
}
#WE PASS THE ACCESS TOKEN TO THE ACTUAL APU WE WANT TO USE TO GET WHAT WE REQUIRE
r2 = requests.get(base_url + api_suffix, headers = headersAuth)
resp_chart = r2.json()

#CONVERTS MESSAGE INTO A JSON
JSON(resp_chart)

#PRINTS ALL OF THE MESSAGE RECEIVED
print(resp_chart)

#PRINTS THE FIRST ELEMENT {} IN THE JSON

print(resp_chart['result'][0])