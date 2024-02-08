import requests
import pandas as pd
from unidecode import unidecode
import os
import urllib3

class GetSourceCatalog:
    def __init__(self, url, headers):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.api_url = url
        self.headers = headers

    def fetch_data_from_api(self):
        response = requests.get(self.api_url, headers=self.headers)
        if response.status_code == 200:
            print('La requête est un succès:', response.status_code)
            data = response.json()
            return data
        else:
            print('La requête a échoué avec le code d\'erreur :', response.status_code)
            return []
    
    def response_to_dataframe(self, data, table_name, download_url, 
                              table_id=None, file_format=None, last_update=None, 
                              dataset_id=None, dataset_name=None,
                              frequency=None):
        processed_data = []
        for table in data:
            content = {
                'id': table.get(table_id),
                'download_URL': table.get(download_url),
                'table_name': table.get(table_name),
                'data_format': table.get(file_format),
                'last_update': table.get(last_update),
                'download_url': table.get(download_url),
                'dataset_id': table.get(dataset_id),
                'dataset_name': table.get(dataset_name),
                'frequency': table.get(frequency)
            }
            processed_data.append(content)
        return pd.DataFrame(processed_data).dropna(subset=['download_URL'])