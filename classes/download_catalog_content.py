import pandas as pd
import os
import re
import requests
from datetime import datetime
import shutil
from google.oauth2 import service_account
from google.cloud import bigquery
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time

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

    def __init__(self, catalog_path = None):
        """
        Initialize the DlCatalogContent object.

        Parameters:
        - catalog_path (str): Path to the CSV file containing the dataset catalog.
        """

        if catalog_path is not None:
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
            shutil.make_archive('data/raw_datasets', 'zip', 'data/raw_datasets')
            print("All files have been zipped into data/datasets.zip")
        except Exception as e:
            print(f"Error when zipping files : {e}")
            return None
        

class GdprSanctionsScrapper:

    # def __init__(self, credentials_path, project_id, catalog_path):
    #     super().__init__(catalog_path)
    #     self.project_id = project_id
    #     self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
    #     self.bq_client = bigquery.Client(credentials=self.credentials, project=project_id)

    # def get_existing_sanction_eu(self):
    #     self.df_eu = self.bq_client.query("SELECT * FROM `cnil-392113.raw_data.sanctions_eu`").result().to_dataframe()
    #     return self.df_eu

    # def get_last_record_eu(self):
    #     self.df_last = self.bq_client.query("SELECT etid_number FROM `cnil-392113.raw_data.sanctions_eu` ORDER BY etid_number DESC").result().to_dataframe()
    #     self.last_sanc_eu = str(self.df_last['etid_number'].iloc[0])
    #     return self.last_sanc_eu
    
    def scrap_eu(self):
        url = 'https://www.enforcementtracker.com/data4sfk3j4hwe324kjhfdwe.json?_=1709659660438'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'keep-alive',
            'Referer': 'https://www.enforcementtracker.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'TE': 'trailers',
        }

        response = requests.get(url, headers=headers)
        data = response.json()['data']


        all_data = []
        for decision in data:
            row_dict = {
                'ETid': decision[1],
                'Country': self.extract_country_name(decision[2]),
                'Authority': decision[3],
                'Date_of_Decision': decision[4],
                'Fine': decision[5],
                'Controller_Processor': decision[6],
                'Sector': decision[7],
                'Quoted_Art.': decision[8],
                'Type': decision[9],
                'Summary': decision[10],
                'Source' : self.extract_href(decision[11]),
            }
            all_data.append(row_dict)
        self.df_eu = pd.DataFrame(all_data)
        return self.df_eu
        
    def extract_country_name(self, string):
        match = re.search(r"alt='([^']+)", string)
        if match:
            # Extract the captured group (country name)
            return match.group(1)
        else:
            # Format not found, return None
            return None

    def extract_href(self, string):
        match = re.search(r"href='([^']+)", string)
        if match:
            # Extract the captured group (href attribute)
            return match.group(1)
        else:
            # Format not found, return None
            return None
