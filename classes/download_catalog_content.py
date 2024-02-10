import pandas as pd
import os
import requests
from datetime import date

class DlCatalogContent:

    def __init__(self, catalog_path):
        self.df_catalog = pd.read_csv(catalog_path)

    def get_tables(self):
        for index, row in self.df_catalog.iterrows():
            if row.download_URL is not None:
                if row.dataset_name is not None:
                  dest_folder = f'data/datasets/{row.dataset_name}'
                  os.makedirs(dest_folder, exist_ok=True)
                else:
                  dest_folder = 'data/datasets/unknown'
                  os.makedirs(dest_folder, exist_ok=True)
                  
                try:
                  response = requests.get(row.download_URL)
                  with open(f'{dest_folder}/{row.table_name}', 'wb') as f:
                          f.write(response.content)
                except Exception as e:
                    print(f"Error when downloading table {row.table_name} : {e}")
                    continue
    