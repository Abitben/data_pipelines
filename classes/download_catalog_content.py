import pandas as pd
import os
import requests
from datetime import datetime
import shutil

class DlCatalogContent:
    """
    Class for downloading and organizing datasets based on a provided catalog.

    Attributes:
    - df_catalog (pd.DataFrame): DataFrame containing the catalog information.

    Methods:
    - __init__(catalog_path): Constructor method that initializes the object with the provided catalog path.
    - reorganize_file_name(file_name, last_date): Helper method to create a new filename with versioning based on the last update date.
    - extract_date(date_str): Helper method to extract and convert date strings to datetime objects.
    - get_tables(): Downloads and organizes datasets based on the information in the catalog.
    - zip_files(): Zips all the downloaded files into a single archive.
    """

    def __init__(self, catalog_path):
        """
        Initialize the DlCatalogContent object.

        Parameters:
        - catalog_path (str): Path to the CSV file containing the dataset catalog.
        """
        self.df_catalog = pd.read_csv(catalog_path)

    def reorganize_file_name(self, file_name, last_date):
        """
        Create a new filename with versioning based on the last update date.

        Parameters:
        - file_name (str): Original filename.
        - last_date (datetime.date): Last update date.

        Returns:
        - str: New filename with versioning.
        """
        base_name, extension = os.path.splitext(file_name)
        new_file_name = f'{base_name}_v{last_date}{extension}'
        return new_file_name

    def extract_date(self, date_str):
        """
        Extract and convert date strings to datetime objects.

        Parameters:
        - date_str (str): Date string in the format '%Y-%m-%dT%H:%M:%S.%f' or '%Y-%m-%dT%H:%M:%S'.

        Returns:
        - datetime.date: Date extracted from the date string.
        """
        try:
            timestamp_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f')
        except:
            timestamp_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
        return timestamp_obj.date()

    def get_tables(self):
        """
        Download and organize datasets based on the information in the catalog.
        """
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
                    last_date = self.extract_date(last_date)
                else:
                    current_datetime = datetime.now()
                    last_date = current_datetime.date()

                try:
                    response = requests.get(row.download_URL)
                    new_file_name = self.reorganize_file_name(row.table_name, last_date)

                    with open(f'{dest_folder}/{new_file_name}', 'wb') as f:
                        f.write(response.content)
                except Exception as e:
                    print(f"Error when downloading table {row.table_name} : {e}")
                    continue
    
    def zip_files(self):
        """
        Zip all the downloaded files into a single archive.
        """
        try:
            shutil.make_archive('data/raw_datasets/raw_datasets', 'zip', 'data/raw_datasets')
            print("All files have been zipped into data/datasets.zip")
        except Exception as e:
            print(f"Error when zipping files : {e}")
            return None