import requests
import pandas as pd
from unidecode import unidecode
import os
from datetime import date
import urllib3

class GetSourceCatalog:
    """
    A class for fetching and processing catalog data from a given API.
    """
    def __init__(self, url, headers):
        """
        Initialize the GetSourceCatalog object with the API URL and headers.

        Parameters:
        - url (str): The URL of the API.
        - headers (dict): Headers to be used in the API request.
        """
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.api_url = url
        self.headers = headers

    def fetch_data_from_api(self):
        """
        Fetch data from the API.

        Returns:
        - list: List of catalog data from the API.
        """
        response = requests.get(self.api_url, headers=self.headers)
        if response.status_code == 200:
            print('Request is a success:', response.status_code)
            data = response.json()
            return data
        else:
            print('Request failed with this error:', response.status_code)
            return []
    
    def response_to_dataframe(self, data, table_name, download_url, 
                              table_id=None, file_format=None, last_update=None, 
                              dataset_id=None, dataset_name=None,
                              frequency=None, accessURL=None):
        """
        Process API response data into a DataFrame.

        Parameters:
        - data (list): List of catalog data.
        - table_name (str): Key for table name in each catalog entry.
        - download_url (str): Key for download URL in each catalog entry.
        - table_id (str): Key for table ID in each catalog entry.
        - file_format (str): Key for file format in each catalog entry.
        - last_update (str): Key for last update information in each catalog entry.
        - dataset_id (str): Key for dataset ID in each catalog entry.
        - dataset_name (str): Key for dataset name in each catalog entry.
        - frequency (str): Key for frequency information in each catalog entry.
        - accessURL (str): Key for access URL in each catalog entry.

        Returns:
        - pd.DataFrame: Processed catalog data in a DataFrame.
        """
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
        """
        Save catalog data to a CSV file.

        Parameters:
        - filename (str): The name of the CSV file.
        """
        os.makedirs('data', exist_ok=True)
        os.makedirs('data/catalog', exist_ok=True)
        path = 'data/catalog/' + filename + '_' + str(date.today()) + '.csv'
        self.df_catalog.to_csv(path, index=False)
        print("CSV file has been loaded to this path", path)

class GetCnilCatalog(GetSourceCatalog):
    """
    A class for fetching and processing catalog data from CNIL with additional information.
    """
    def __init__(self, url, headers, url_additional_info):
        """
        Initialize the GetCnilCatalog object.

        Parameters:
        - url (str): The URL of the CNIL API.
        - headers (dict): Headers to be used in the API request.
        - url_additional_info (str): URL of the additional information CSV file.
        """
        super().__init__(url, headers)
        self.additional_info = url_additional_info

    def load_additional_info(self):
        """
        Load additional information from a CSV file.

        Returns:
        - pd.DataFrame: Additional information loaded from the CSV file.
        """
        try:
            self.df_dataset = pd.read_csv(self.additional_info, sep=';')
            return self.df_dataset
        except Exception as e:
            print(f"Error when loading CSV file : {e}")
            return None
        
    def identify_datasets_info(self): 
        """
        Identify dataset information and add it to the catalog DataFrame.
        """
        def find_dataset_id(row):
            for dataset_id in self.df_dataset.id:
                if dataset_id in row['accessURL']:
                    return dataset_id

        self.df_catalog['dataset_id'] = self.df_catalog.apply(lambda row: find_dataset_id(row), axis=1)
        return self.df_catalog

    def merge_additional_info(self):
        """
        Merge additional information into the catalog DataFrame.
        """
        self.df_catalog = self.df_catalog.merge(self.df_dataset[['id', 'slug', 'frequency']], left_on='dataset_id', right_on='id', how='left')
        self.df_catalog = self.df_catalog.drop(columns=['id', 'frequency_x'])
        self.df_catalog.rename(columns={'frequency_y': 'frequency', 'slug': 'dataset_name'}, inplace=True)
        self.df_catalog.dropna(subset=['table_id'], inplace=True)
        self.df_catalog = self.df_catalog.reset_index()
        return self.df_catalog
