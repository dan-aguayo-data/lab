import requests
import json
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

# SET YOUR PARAMETERS HERE
base_url = superset_dev_url
username = superset_dev_user
password = superset_dev_password

# STEP 1: LOGIN TO GET ACCESS TOKEN
login_payload = {
    'username': username,
    'password': password,
    'provider': 'db'
}

login_response = requests.post(base_url + 'api/v1/security/login', json=login_payload)
if login_response.status_code != 200:
    print("Failed to login. Check your credentials and URL.")
    print(login_response.text)
    exit()

access_token = login_response.json().get('access_token')
if not access_token:
    print("Failed to extract access token from the login response.")
    exit()

headersAuth = {
    'Authorization': f'Bearer {access_token}'
}

# STEP 2: RETRIEVE CHARTS
chart_list_url = base_url + 'api/v1/chart/'
chart_list_response = requests.get(chart_list_url, headers=headersAuth)

if chart_list_response.status_code == 200:
    charts_data = chart_list_response.json()
    charts = charts_data.get('result', [])
    print("Charts retrieved successfully!")
    if not charts:
        print("No charts found.")
        exit()
    # Extract chart IDs
    chart_ids = [chart['id'] for chart in charts]
    print("Chart IDs to export:", chart_ids)
else:
    print(f"Failed to fetch charts: {chart_list_response.status_code}")
    print(chart_list_response.text)
    exit()

# STEP 3: EXPORT CHARTS
# The export endpoint is GET and needs a 'q' parameter with a JSON list of IDs
export_url = base_url + 'api/v1/chart/export/'
params = {
    'q': json.dumps(chart_ids)  # Must encode the IDs as a JSON string
}

export_response = requests.get(export_url, headers=headersAuth, params=params)

if export_response.status_code == 200:
    # The response should be a ZIP file.
    # Save it to disk:
    with open('charts_export.zip', 'wb') as f:
        f.write(export_response.content)
    print("Export successful! The charts have been saved to 'charts_export.zip'.")
else:
    print(f"Failed to export charts: {export_response.status_code}")
    print(export_response.text)
