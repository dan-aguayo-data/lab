import pandas as pd
import json
import requests
from IPython.display import JSON
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


#Define function to get superset password

def get_password(keyvaulturl,secretname):
    # Set the Key Vault URL
    key_vault_url = keyvaulturl

    # Authenticate using DefaultAzureCredential
    # This works with Managed Identity, Azure CLI authentication, or environment variables
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=key_vault_url, credential=credential)

    # Get the secret from Key Vault
    secret_name = secretname
    try:
        retrieved_secret = client.get_secret(secret_name)
        superset_password = retrieved_secret.value
        print("Retrieved secret successfully!")
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        superset_password = None

    # Use the password in your application
    return superset_password


#Assign password value in superset
vault_url = "https://ces-snowflake-keys.vault.azure.net/"

superset_dev_password = get_password(vault_url,"superset-dev-password")
superset_dev_user = get_password(vault_url,"superset-dev-user")
superset_dev_url = get_password(vault_url,"superset-dev-url")

#SET YOUR PARAMETERS HERE #
base_url = superset_dev_url
#'http://localhost:8088/'
username = superset_dev_user
password = superset_dev_password
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