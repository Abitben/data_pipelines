import requests
import pandas as pd
from unidecode import unidecode
import os
from datetime import date
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
                              frequency=None, accessURL=None):
        processed_data = []
        for table in data:
            content = {
                'table_id': table.get(table_id),
                'table_name': table.get(table_name),
                'download_URL': table.get(download_url),
                'accessURL': table.get(accessURL),
                'data_format': table.get(file_format),
                'last_update': table.get(last_update),
                'dataset_id': table.get(dataset_id),
                'dataset_name': table.get(dataset_name),
                'frequency': table.get(frequency)
            }
            processed_data.append(content)
          
        self.df_catalog = pd.DataFrame(processed_data).sort_values(by=['last_update', 'table_name', 'download_URL'], ascending=False)
        return self.df_catalog
    
    def save_to_csv(self, filename):
      os.makedirs('data', exist_ok=True)
      os.makedirs('data/catalog', exist_ok=True)
      path = 'data/catalog/' + filename + '_' + str(date.today()) + '.csv'
      self.df_catalog.to_csv(path, index=False)
      print("Le fichier catalog a bien été enregistré ici", path)

class GetCnilCatalog(GetSourceCatalog):
    def __init__(self, url, headers, url_additional_info):
        super().__init__(url, headers)
        self.additional_info = url_additional_info

    def load_additional_info(self):
        try:
            self.df_dataset = pd.read_csv(self.additional_info, sep=';')
            return self.df_dataset
        except Exception as e:
            print(f"Erreur lors du chargement du fichier CSV : {e}")
            return None
        
    def identify_datasets_info(self): 
        def find_dataset_id(row):
            for dataset_id in self.df_dataset.id:
                if dataset_id in row['accessURL']:
                    return dataset_id

        self.df_catalog['dataset_id'] = self.df_catalog.apply(lambda row: find_dataset_id(row), axis=1)
        return self.df_catalog

    def merge_additional_info(self):
        self.df_catalog = self.df_catalog.merge(self.df_dataset[['id', 'slug', 'frequency']], left_on = 'dataset_id', right_on='id', how='left')
        self.df_catalog = self.df_catalog.drop(columns=['id', 'frequency_x'])
        self.df_catalog.rename(columns={'frequency_y': 'frequency', 'slug' : 'dataset_name'}, inplace=True)
        self.df_catalog.dropna(subset=['table_id'], inplace=True)
        self.df_catalog = self.df_catalog.reset_index()
        return self.df_catalog