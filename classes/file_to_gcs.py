from .connectors import GoogleConnector
from colorama import Fore, Style
from google.oauth2 import service_account
from google.cloud import storage
import requests
from io import BytesIO
import io
import pandas as pd
import zipfile
import gzip
from datetime import date
import os

class FromFileToGCS(GoogleConnector):
    """
    A class used to process data from a URL or a local file and upload it to Google Cloud Storage (GCS)

    ...

    Attributes
    ----------
    bucket_name : str
        a formatted string to print out the name of the GCS bucket
    credentials_path : str
        a formatted string that holds the path of the service account credentials file
    url : str
        a formatted string that holds the URL of the data to be processed
    credentials : google.auth.credentials.Credentials
        service account credentials, which are used for authentication
    storage_client : google.cloud.storage.client.Client
        the client for interacting with the GCS API

    Methods
    -------
    create_bucket():
        Creates a new bucket in GCS
    download_and_upload_from_URL(url, destination_blob_name):
        Downloads data from a URL and uploads it to GCS
    local_to_gcs(file_path, destination_blob_name):
        Uploads a local file to GCS
    list_blobs():
        Extracts data from a compressed file and uploads it to GCS
    extract_and_upload_sel(blobs):
        Extracts data from a compressed file and uploads it to GCS
    """

    def __init__(self, credentials_path, bucket_name, project_id=None):
        """
        Constructs all the necessary attributes for the DataProcessor object.

        Parameters
        ----------
            bucket_name : str
                name of the GCS bucket
            credentials_path : str
                path of the service account credentials file
        """
        super().__init__(credentials_path, project_id)
        self.bucket_name = bucket_name

    def create_bucket(self):
        """
        Creates a new bucket in GCS. If the bucket already exists, it prints a message and does nothing.
        """

        try:
            self.storage_client.get_bucket(self.bucket_name).exists()
            print("Bucket already exists.")
        except:
            bucket = self.storage_client.bucket(self.bucket_name)
            # set the storage class to COLDLINE, which is the cheapest one
            bucket.storage_class = "COLDLINE"
            # set the location to europe-west1, which is in the EU
            new_bucket = self.storage_client.create_bucket(bucket, location="europe-west1")
            print('A new bucket created at {}'.format(new_bucket.name))

    def download_and_upload_from_URLs(self, urls, dest_folder, dest_blob=None):
        """
        Downloads data from multiple URLs and uploads them to GCS.

        Parameters
        ----------
            urls : list of str
                URLs of the data to be downloaded
            dest_folder : str
                name of the folder inside the bucket where the data will be uploaded in GCS
        """

        today = str(date.today()) + "/"
        dest_folder = dest_folder + "/"
        if dest_blob is None:
            for url in urls:
                response = requests.get(url)
                if response.status_code == 200:
                    file_stream = BytesIO(response.content)
                    file_name = os.path.basename(url)
                    destination_blob_name_raw = today + dest_folder + file_name
                    bucket = self.storage_client.bucket(self.bucket_name)
                    blob = bucket.blob(destination_blob_name_raw)
                    blob.upload_from_file(file_stream, content_type='application/zip')
                    print(f"{Fore.GREEN}Raw file {file_name} downloaded and uploaded to GCS successfully to {destination_blob_name_raw}.{Style.RESET_ALL}")
                else:
                    print(f"{Fore.RED}Request for {url} failed with error {response.status_code}.{Style.RESET_ALL}")
        else:
            for url, destination_blob_name in zip(urls, dest_blob):
                response = requests.get(url)
                if response.status_code == 200:
                    file_stream = BytesIO(response.content)
                    destination_blob_name_raw = today + dest_folder + destination_blob_name
                    bucket = self.storage_client.bucket(self.bucket_name)
                    blob = bucket.blob(destination_blob_name_raw)
                    blob.upload_from_file(file_stream, content_type='application/zip')
                    print(f"{Fore.GREEN}Raw file {destination_blob_name} downloaded and uploaded to GCS successfully to {destination_blob_name_raw}.{Style.RESET_ALL}")
                else:
                    print(f"{Fore.RED}Request for {url} failed with error {response.status_code}.{Style.RESET_ALL}")

    def local_to_gcs(self, file_paths, dest_folder, dest_blob=None):
        """
            Uploads multiple local files to GCS.

            Parameters
            ----------
                file_paths : list of str
                    paths of the local files to be uploaded
                dest_folder : str
                    name of the folder inside the bucket where the data will be uploaded in GCS
            """
        today = str(date.today()) + "/"
        dest_folder = dest_folder + "/"

        if dest_blob is None:
            for file_path in file_paths:
                print(file_path)
                if type(file_path) == str:
                    file_name = os.path.basename(file_path)
                    destination_blob_name_raw = today + dest_folder + file_name
                    bucket = self.storage_client.bucket(self.bucket_name)
                    blob = bucket.blob(destination_blob_name_raw)
                    blob.upload_from_filename(file_path, timeout=300)
                    print(f"{Fore.GREEN} file {file_name} uploaded to GCS successfully to {destination_blob_name_raw}.{Style.RESET_ALL}")
                elif type(file_path) == io.BytesIO or type(file_path) == io.StringIO:
                    raise ValueError("BytesIO or StringIO cannot be uploaded without a destination_blob_name. Try using dest_blob parameter or save it as a csv to your local system.")
                elif isinstance(file_path, pd.DataFrame):
                    raise ValueError("Dataframe cannot be uploaded without a destination_blob_name. Try using dest_blob parameter or save it as a csv to your local system.")
        else:
            for file_path, destination_blob_name in zip(file_paths, dest_blob):
                if type(file_path) == str:
                    file_name = os.path.basename(file_path)
                    destination_blob_name_raw = today + dest_folder + file_name
                    bucket = self.storage_client.bucket(self.bucket_name)
                    blob = bucket.blob(destination_blob_name_raw)
                    blob.upload_from_filename(file_path, timeout=300)
                    print(f"{Fore.GREEN} file {file_name} uploaded to GCS successfully to {destination_blob_name_raw}.{Style.RESET_ALL}")
                elif isinstance(file_path, pd.DataFrame):
                    data = file_path
                    destination_blob_name_raw = today + dest_folder + destination_blob_name
                    csv_data = data.to_csv(index=False)
                    bucket = self.storage_client.bucket(self.bucket_name)
                    blob_output = bucket.blob(destination_blob_name_raw)
                    blob_output.upload_from_string(csv_data, content_type='text/csv')
                    print(f"{Fore.GREEN}{destination_blob_name} is uploaded to {destination_blob_name_raw}.{Style.RESET_ALL}")
                elif type(file_path) == io.BytesIO or type(file_path) == io.StringIO:    
                    destination_blob_name_raw = today + dest_folder + destination_blob_name
                    bucket = self.storage_client.bucket(self.bucket_name)
                    blob = bucket.blob(destination_blob_name_raw)
                    blob.upload_from_file(file_path, content_type='application/zip')
                    print(f"{Fore.GREEN} file {destination_blob_name} uploaded to GCS successfully to {destination_blob_name_raw}.{Style.RESET_ALL}")
            

    def list_blobs(self, prefix = None):
        """
        Extracts data from a compressed file and uploads it to GCS.
        """
        bucket = self.storage_client.get_bucket(self.bucket_name)
        print(bucket.name)
        blobs = list(self.storage_client.list_blobs(self.bucket_name, prefix=prefix))
        return blobs

    def extract_and_upload_sel(self, blobs):
        """
        Extracts data from a compressed file and uploads it to GCS.
        Parameters
        ----------
            blobs : list
                list of blobs to be extracted and uploaded to GCS
        """
        self.blobs = blobs
        bucket = self.storage_client.get_bucket(self.bucket_name)
        for blob in blobs:
            print(f"{Fore.CYAN}Start extracting and uploading to GCS : {blob.name}{Style.RESET_ALL}")
            zip_data = blob.download_as_bytes()

            if self.destination_blob_name.endswith('.zip'):
                with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
                    for file_name in z.namelist():
                        with z.open(file_name) as f:
                            data = pd.read_csv(f)
                            file_name_clean = file_name.replace(".zip", "")
                            destination_blob_name = f'raw_csv/{file_name_clean}.csv'
                            csv_data = data.to_csv(index=False)
                            blob_output = bucket.blob(destination_blob_name)
                            blob_output.upload_from_string(csv_data, content_type='text/csv')
                            print(f"{Fore.GREEN}{self.destination_blob_name_raw} is uncompressed and uploaded to {destination_blob_name}.{Style.RESET_ALL}")

            elif self.destination_blob_name.endswith('.gz'):
                with gzip.GzipFile(fileobj=io.BytesIO(zip_data)) as f:
                    data = pd.read_csv(f)
                    file_name_clean = self.destination_blob_name.replace(".gz", "")
                    destination_blob_name = f'raw_csv/{file_name_clean}.csv'
                    csv_data = data.to_csv(index=False)
                    blob_output = bucket.blob(destination_blob_name)
                    blob_output.upload_from_string(csv_data, content_type='text/csv')
                    print(f"{Fore.GREEN}{self.destination_blob_name_raw} is uncompressed and uploaded to {destination_blob_name}.{Style.RESET_ALL}")

            elif self.destination_blob_name.endswith('.csv'):
                with io.BytesIO(zip_data) as f:
                    data = pd.read_csv(f)
                    destination_blob_name = f'raw_csv/{destination_blob_name}'
                    csv_data = data.to_csv(index=False)
                    blob_output = bucket.blob(destination_blob_name)
                    blob_output.upload_from_string(csv_data, content_type='text/csv')
                    print(f"{Fore.GREEN}{self.destination_blob_name_raw} is uncompressed and uploaded to {destination_blob_name}.{Style.RESET_ALL}")