from .connectors import GoogleConnector
from google.cloud import bigquery
import pandas_gbq
from io import StringIO
from colorama import Fore, Style
import requests
from io import BytesIO
import io
import pandas as pd
import zipfile
import gzip
import zipfile
import re
from unidecode import unidecode

class FromGCStoGBQ(GoogleConnector):
    """
    A class used to process data from Google Cloud Storage (GCS) and BigQuery (BQ)

    ...

    Attributes
    ----------
    credentials : google.auth.credentials.Credentials
        Google Cloud credentials from a service account file
    bq_client : google.cloud.bigquery.client.Client
        A BigQuery client object
    storage_client : google.cloud.storage.client.Client
        A Cloud Storage client object
    project_id : str
        Google Cloud project ID
    dataset_name : str
        BigQuery dataset name
    bucket_name : str
        Cloud Storage bucket name

    Methods
    -------
    create_dataset():
        Creates a new BigQuery dataset
    list_blobs():
        Lists all blobs in the Cloud Storage bucket
    upload_to_bq(blobs):
        Uploads blobs data to BigQuery
    """

    def __init__(self, credentials_path, project_id, dataset_id, bucket_name=None):
        """
        Constructs all the necessary attributes for the GCS_BQ_Processor object.

        Parameters
        ----------
            credentials_path : str
                Path to the service account file
            project_id : str
                Google Cloud project ID
            dataset_name : str
                BigQuery dataset name
            bucket_name : str
                Cloud Storage bucket name
        """
        super().__init__(credentials_path, project_id=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bucket_name = bucket_name
    

    def create_dataset(self):
        """
        Creates a new BigQuery dataset
        """
        dataset_id = f"{self.project_id}.{self.dataset_id}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "EU"
        self.bq_client.create_dataset(dataset, timeout=30, exists_ok=True)
        print(f"{Fore.GREEN}Created dataset (or already exists) {self.bq_client.project}.{dataset.dataset_id}{Style.RESET_ALL}")

    def list_blobs(self, prefix=None):
        """
        Lists all blobs in the Cloud Storage bucket

        Returns
        -------
        list
            a list of blobs in the bucket
        """
        self.prefix = prefix
        bucket = self.storage_client.get_bucket(self.bucket_name)
        print(bucket.name)
        blobs = list(self.storage_client.list_blobs(self.bucket_name, prefix=self.prefix))
        return blobs

    def upload_to_bq(self, blobs, prefix=None):
        """
        Uploads blobs data to BigQuery

        Parameters
        ----------
        blobs : list
            a list of blobs to be uploaded
        """
        for blob in blobs:
            blob_data = blob.download_as_text()
            df = pd.read_csv(StringIO(blob_data))
            print(f"{Fore.BLUE}{blob.name}{Style.RESET_ALL}")
            blob_name = blob.name.replace(f"raw_csv/", "").replace(".csv", "")
            table_name = self.project_id + '.' + "raw_data" + "." + blob_name
            pandas_gbq.to_gbq(df, table_name, project_id=self.project_id, if_exists='replace', api_method= "load_csv")
            print(f"{Fore.GREEN}{blob.name} is uploaded to {table_name}{Style.RESET_ALL}")

    def upload_zip_to_bq(self, zip_file):
        file_list = [file for file in zip_file.namelist()]
        filtered_list = list(filter(lambda x: not x.endswith('/'), file_list))
        for filename in filtered_list:
            if "/" in filename: 
                print("---------------------")
                print(filename)
                filename_bq = filename.split('/')[1]
                print(filename_bq)
                pattern = "_(?=\d{4}_\d{2}_\d{2})"
                split_name = re.split(pattern, filename_bq)
                filename_bq = unidecode(split_name[0]).lower()
                date_ext = split_name[1]
                date_ext = date_ext.replace('.csv', '')
                if "_csv" in date_ext:
                    ext = "csv"
                    date_date = date_ext.split('_')[0] + '-' + date_ext.split('_')[1] + '-' + date_ext.split('_')[2]
                    print(date_date)
                    print(ext)
                elif "_xlsx" in date_ext:
                    ext = "xlsx"
                    date_date = date_ext.split('_')[0] + '-' + date_ext.split('_')[1] + '-' + date_ext.split('_')[2]
                    print(date_date)
                    print(ext)
                else:
                    ext = "csv"
                    date_date = date_ext
                    print(date_date)
                    print(ext)
                print(filename_bq)
            else:
                filename_bq = filename.replace('.csv', '')
                filename_bq = unidecode(filename_bq).lower()
            table_name = self.project_id + '.' + self.dataset_id + "." + filename_bq
            print('this is the table name: ', table_name)
            print("---------------------")
                
            with zip_file.open(filename) as myfile:
                try:
                  df = pd.read_csv(myfile, sep=";")
                  pandas_gbq.to_gbq(df, table_name, project_id=self.project_id, if_exists='replace', api_method= "load_csv")
                  print(f"{Fore.GREEN}{filename} is uploaded to {table_name}{Style.RESET_ALL}")
                except Exception as e:
                  print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")  
                  print('try to read with sep=","')
                  df = pd.read_csv(myfile)
                  pandas_gbq.to_gbq(df, table_name, project_id=self.project_id, if_exists='replace', api_method= "load_csv")
                  print(f"{Fore.GREEN}{filename} is uploaded to {table_name}{Style.RESET_ALL}")
    
    def upload_zipio_to_bq(self, zip_file_io):
        with zipfile.ZipFile(zip_file_io, 'r') as zip_file:
            self.upload_zip_to_bq(zip_file)


    def df_to_bq(self, df, table_name):
        """
        Uploads a DataFrame to BigQuery

        Parameters
        ----------
        df : pandas.DataFrame
            a DataFrame to be uploaded
        table_name : str
            a name of the table in BigQuery
        """
        table_name = self.project_id + '.' + self.dataset_id + "." + table_name
        pandas_gbq.to_gbq(df, table_name, project_id=self.project_id, if_exists='replace', api_method= "load_csv")
        print(f"{Fore.GREEN}DataFrame is uploaded to {table_name}{Style.RESET_ALL}")