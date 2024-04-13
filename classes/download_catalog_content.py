import pandas as pd
import os
import re
import requests
from datetime import datetime
import shutil
from google.oauth2 import service_account
from google.cloud import bigquery
import time
from bs4 import BeautifulSoup
from .connectors import GoogleConnector
from pandas.errors import ParserError
from google.oauth2 import service_account
from google.cloud import storage
import zipfile
import tempfile
import io
from colorama import Fore, Style
from unidecode import unidecode
from IPython.display import display


class DlCatalogContentLocal:
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

        print(Fore.GREEN + "All files have been downloaded and organized in data/raw_datasets folder." + Style.RESET_ALL)
    
    def zip_files(self):
        """
        Zip all the downloaded files into a single archive.
        """
        try:
            shutil.make_archive('data/raw_datasets', 'zip', 'data/raw_datasets')
            print(Fore.GREEN + "All files have been zipped into data/raw_datasets.zip" + Style.RESET_ALL)
        except Exception as e:
            print(Fore.RED + f"Error when zipping files : {e}" + Style.RESET_ALL)
            return None
        
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
    

class DLFromGCSCatalogToZip(GoogleConnector):
    def __init__(self, gcs_bucket_name, credentials_path, zip_blob_name, project_id = None):
        super().__init__(credentials_path, project_id)
        self.bucket_name = gcs_bucket_name
        self.gcs_bucket_name = gcs_bucket_name
        self.zip_blob_name = zip_blob_name
    
    def get_file_io(self):
        bucket = self.storage_client.get_bucket(self.gcs_bucket_name)
        blob = bucket.blob(self.zip_blob_name)
        return io.BytesIO(blob.download_as_string())
    
    def download_files_to_zip_io(self):
        csv_catalog = self.get_file_io()
        df_catalog = pd.read_csv(csv_catalog)

        files = []
        for index, row in df_catalog.iterrows():
            table_name = row.table_name

            if row.dataset_name is not None:
              dest_folder = row.dataset_name
            else:
              dest_folder = 'unknown'

            if row.last_update is not None:
                last_date = row.last_update
                last_date = self.extract_date(last_date)
            else:
                current_datetime = datetime.now()
                last_date = current_datetime.date()

            file_path = f'{dest_folder}/{table_name}_{last_date}'

            if row.download_URL and not pd.isna(row.download_URL):
              url = row.download_URL
              response = requests.get(url)
              if response.status_code == 200:
                  print('current file downloading :', file_path)
                  files.append((file_path, response.content))
              else:
                  print(f"Failed to download file from {url}")
        return files
    
    def create_zip(self, files):
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
            for file_path, content in files:
                print('current file :', file_path)
                directory, filename = os.path.split(file_path)
                zip_file.writestr(file_path, content)
        zip_buffer.seek(0)
        return zip_buffer
    
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
    
        

class GdprSanctionsScrapper:
    """ 
    A simple web scrapper to extract GDPR sanctions from the EU and FR authorities.
    """

    
    def scrap_eu(self):
        """
        Scrap the GDPR sanctions from the EU authorities.
        """


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
        """
        Extract the country name from the string for the EU sanctions.
        """

        match = re.search(r"alt='([^']+)", string)
        if match:
            # Extract the captured group (country name)
            return match.group(1)
        else:
            # Format not found, return None
            return None

    def extract_href(self, string):
        """
        Extract the href attribute from the string for the EU sanctions.
        """
        match = re.search(r"href='([^']+)", string)
        if match:
            # Extract the captured group (href attribute)
            return match.group(1)
        else:
            # Format not found, return None
            return None

    def get_sanctions_fr(self):
        """
        Extract the GDPR sanctions from the CNIL website upto 2023.
        """
        response = requests.get("https://www.cnil.fr/fr/les-sanctions-prononcees-par-la-cnil")
        url_content = response.content
        soup = BeautifulSoup(url_content)
        tbody_list = soup.find_all('tbody')


        data = []
        for tbody in tbody_list:
            rows = tbody.find_all('tr')
            for index, row in enumerate(rows):
                cells = row.find_all(['td'])
                if len(cells) == 0:
                    pass
                elif len(cells) == 4:
                    decision = {
                        'date': cells[0].text,
                        'organisme_type': cells[1].text,
                        'manquements': cells[2].text,
                        'decision': cells[3].text,
                        'theme': None
                            }
                elif len(cells) == 5:
                    decision = {
                        'date': cells[0].text,
                        'organisme_type': cells[1].text,
                        'manquements': cells[3].text,
                        'decision': cells[4].text, 
                        'theme': cells[2].text
                        }
                data.append(decision)

        df_fr = pd.DataFrame(data)
        return df_fr