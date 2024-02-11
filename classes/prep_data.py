import pandas as pd
from google.oauth2 import service_account
from google.cloud import storage
import zipfile
import io

class ZipFileProcessor:
  def __init__(self, gcs_bucket_name, credentials_path, zip_blob_name, output_folder_name):
    self.gcs_bucket_name = gcs_bucket_name
    self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
    self.storage_client = storage.Client(credentials=self.credentials)
    self.zip_blob_name = zip_blob_name
    self.output_folder_name = output_folder_name

  def process_zip_file(self):
    # Create a client to interact with GCS
    client = storage.Client()

    # Get the GCS bucket
    bucket = client.get_bucket(self.gcs_bucket_name)

    # Get the zip blob
    blob = bucket.blob(self.zip_blob_name)

    # Download the zip blob as bytes
    zip_bytes = blob.download_as_bytes()

    # Create a file-like object from the zip bytes
    zip_file = zipfile.ZipFile(io.BytesIO(zip_bytes))

    # Iterate over each file in the zip
    for file_name in zip_file.namelist():
      # Open the file with pandas
      with zip_file.open(file_name) as file:
        df = pd.read_csv(file)

      # Save the file to the output folder within GCS
      output_blob_name = f"{self.output_folder_name}/{file_name}"
      output_blob = bucket.blob(output_blob_name)
      output_blob.upload_from_string(df.to_csv(index=False))

    print("Zip file processed successfully!")