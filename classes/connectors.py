from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage

class GoogleConnector:
    
    def __init__(self, credentials_path, project_id):
        self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
        self.bq_client = bigquery.Client(credentials=self.credentials, project=project_id)
        self.storage_client = storage.Client(credentials=self.credentials)
