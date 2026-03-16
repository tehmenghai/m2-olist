from google.cloud import storage
from google.oauth2 import service_account

creds = service_account.Credentials.from_service_account_file(
    "dashboards/lik_hong/config/service_account.json"
)
client = storage.Client(project="dsai-m2-gcp", credentials=creds)
try:
    bucket = client.create_bucket("dsai-m2-gcp-bronze", location="US")
    print(f"Bucket {bucket.name} created in {bucket.location}")
except Exception as e:
    print(f"Error: {e}")
