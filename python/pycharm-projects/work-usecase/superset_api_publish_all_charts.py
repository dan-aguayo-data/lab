import requests

# Set your Superset instance details
base_url = "https://your-superset-instance.com/"
username = "admin"
password = "your_password"

# Authenticate and get access token
login_payload = {
    "username": username,
    "password": password,
    "provider": "db"
}
login_response = requests.post(f"{base_url}api/v1/security/login", json=login_payload)
access_token = login_response.json().get("access_token")

headers = {
    "Authorization": f"Bearer {access_token}"
}

# Import the ZIP file
zip_file_path = "charts_export.zip"
with open(zip_file_path, "rb") as f:
    files = {"formData": ("charts_export.zip", f, "application/zip")}
    import_response = requests.post(f"{base_url}api/v1/chart/import/", headers=headers, files=files)

if import_response.status_code == 200:
    print("Charts imported successfully!")
else:
    print(f"Failed to import charts: {import_response.status_code}")
    print(import_response.text)
