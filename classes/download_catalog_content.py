import pandas as pd
import os
import requests
from datetime import date
from .list_files import FolderLister

class DlCatalogContent(FolderLister):

    def __init__(self, catalog_path):
        self.df_catalog = pd.read_csv(catalog_path)

    def get_tables(self):
        for index, row in self.df_catalog.iterrows():
            if row.download_URL is not None:
                if row.dataset_name is not None:
                  dest_folder = f'data/raw_datasets/{row.dataset_name}'
                  os.makedirs(dest_folder, exist_ok=True)
                else:
                  dest_folder = 'data/raw_datasets/unknown'
                  os.makedirs(dest_folder, exist_ok=True)
            
                if row.last_update is not None:
                    last_date = row.last_update
                else:
                    last_date = date.today()

                try:
                  response = requests.get(row.download_URL)
                  with open(f'{dest_folder}/{row.table_name}_v{last_date}', 'wb') as f:
                          f.write(response.content)
                except Exception as e:
                    print(f"Error when downloading table {row.table_name} : {e}")
                    continue
                
    def zip_files(self):
      """
      Zip all the files in the datasets folder.
      """
      try:
          os.system('zip -r data/raw_datasets.zip data/raw_datasets')
          print("All files have been zipped into data/datasets.zip")
      except Exception as e:
          print(f"Error when zipping files : {e}")
          return None


    