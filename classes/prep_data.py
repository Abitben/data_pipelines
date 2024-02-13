import pandas as pd
from google.oauth2 import service_account
from google.cloud import storage
import zipfile
import os
import io
from colorama import Fore, Style
import re
from unidecode import unidecode


class ZipFileProcessor:
    def __init__(self, gcs_bucket_name, credentials_path, zip_blob_name, output_folder_name):
        self.gcs_bucket_name = gcs_bucket_name
        self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
        self.storage_client = storage.Client(credentials=self.credentials)
        self.zip_blob_name = zip_blob_name
        self.output_folder_name = output_folder_name

    def process_zip_file(self):
        client = storage.Client()
        bucket = client.get_bucket(self.gcs_bucket_name)
        blob = bucket.blob(self.zip_blob_name)
        zip_bytes = blob.download_as_bytes()
        zip_file = zipfile.ZipFile(io.BytesIO(zip_bytes))

        file_list = [file for file in zip_file.namelist()]
        filtered_list = list(filter(lambda x: not x.endswith('/'), file_list))

        for file_name in filtered_list:
            print(Fore.GREEN + 'current:', file_name + Style.RESET_ALL)
            df = self.load_and_process_file(zip_file, file_name, bucket)

        print("Zip file processed successfully!")

    def load_and_process_file(self, zip_file, file_name, bucket):
        try:
            print('extension found')
            if file_name.endswith('.csv'):
                df = self.process_csv_file(zip_file, file_name, bucket)
                return df
            elif file_name.endswith('.xlsx'):
                df = self.process_excel_file(zip_file, file_name, bucket)
                return df
            else:
                print('no extension found')
                try:
                  df = self.process_csv_file(zip_file, file_name, bucket)
                  return df
                except:
                  df = self.process_excel_file(zip_file, file_name, bucket)
                  return df
        except Exception as e:
            print(Fore.RED + f"Error when processing file {file_name} : {e}" + Style.RESET_ALL)

    def process_csv_file(self, zip_file, file_name, bucket):
        with zip_file.open(file_name) as file:
            try: 
              df = pd.read_csv(file)
              print('read_csv with ,', 'shape :', df.shape, 'columns', df.columns)
            except:
              df = pd.read_csv(file, sep=";", on_bad_lines='warn')
              print('read_csv with ;', 'shape :', df.shape, 'columns', df.columns)
            return df

    def process_excel_file(self, zip_file, file_name, bucket):
        with zip_file.open(file_name) as file:
            df = pd.read_excel(file)
            print('read_excel', 'shape :', df.shape, 'columns', df.columns)
            return df
    
    def reorganize_columns(self, df):
      if self.df.shape[1] > self.df.shape[0]:
            print('Need to transpose')
            df = self.df.transpose()
            df.columns = self.df.iloc[0]
            df = self.df[1:]
            df = self.df.reset_index(names=[self.df.columns.name])
            df.columns.name = ""
            return self.df

    def upload_to_gcs(self, bucket, file_name, df):
        output_blob_name = f"{self.output_folder_name}/{file_name}"
        output_blob = bucket.blob(output_blob_name)
        output_blob.upload_from_string(df.to_csv(index=False))
        print(f"{file_name} processed and uploaded to GCS.")


class PrepFilesBQ:

    def __init__(self, paths):
        self.paths = paths

    def process_files(self):
        for path in self.paths:
            print(path)
            try:
                print('extension found')
                if path.endswith('.csv'):
                    df = self.open_csv_file(path)
                    self.process_df(df, path)
                elif path.endswith('.xlsx'):
                    df = self.open_excel_file(path)
                    self.process_df(df, path)
                else:
                    print('no extension found')
                    try:
                        df = self.open_csv_file(path)
                        self.process_df(df, path)
                    except:
                        df = self.open_excel_file(path)
                        self.process_df(df, path)
            except Exception as e:
                print(Fore.RED + f"Error when processing file {path} : {e}" + Style.RESET_ALL)
    
    def open_csv_file(self, path):
        try: 
            df = pd.read_csv(path)
            print('read_csv with ,', 'shape :', df.shape, 'columns', df.columns)
        except:
            df = pd.read_csv(path, sep=";", on_bad_lines='warn')
            print('read_csv with ;', 'shape :', df.shape, 'columns', df.columns)
        return df

    def open_excel_file(self, path):
        df = pd.read_excel(path)
        print('read_excel', 'shape :', df.shape, 'columns', df.columns)
        return df
    
    def process_df(self, df, path):
        df = self.transpose_df(df)
        df = self.columns_formatter(df)
        df = self.check_column_clean(df)
        self.return_csv(df, path)

    def transpose_df(self, df):
        if df.shape[1] > df.shape[0]:
            print('Need to transpose')
            df = df.transpose()
            df.columns = df.iloc[0]
            df = df[1:]
            # df = df.reset_index(names=[df.columns.name])
            # df.columns.name = ""
            return df
        else:
            print('No need to transpose')
            return df
        
    def columns_formatter(self, df):
        new_columns = []
        for index, column in enumerate(df.columns):
            try:
                # Remplacement des caractères spéciaux
                column = re.sub(r'[!\"$()\*\.,\/;?\@\[\]\\^`{}\~]', '', column)

                # Remplacement des espaces par des underscores et conversion en minuscules
                column = column.strip().lower().replace(" ", "_")

                # Suppression des caractères spéciaux accentués
                column = unidecode(column)

                # Remplacement de certains caractères spéciaux par des underscores
                column = column.replace("\n", "_")
                column = column.replace("'", "")
                column = column.replace("-", "")
                column = column.replace("&", 'and')
                
                # Suppression de certaines chaînes spécifiques dans les noms de colonnes
                column = column.replace('https:edpbeuropaeuaboutedpbboardmembers_fr', "").replace('https:wwwafapdporglafapdpmembres', "")
                
                # Suppression des chevrons '<' et '>'
                column = column.replace('<', "").replace('>', "")

                # Limiter la longueur du nom de la colonne à 200 caractères
                column = column[:200]

                new_columns.append(column)
            except AttributeError:
                new_columns.append(column)
                continue
    
        df.columns = new_columns
        return df
    
    def check_column_clean(self, df):
        for index, column in enumerate(df.columns):
            try: 
                pattern = r"^[a-zA-Z0-9_]+$"
                is_cleaned = re.match(pattern, column)

                if is_cleaned and len(column) <= 200:
                    
                    print("The column_formatter method worked perfectly.")
                else:
                    print("The column_formatter method didn't work perfectly. Executing again the method...")
                    # Re-run the column_clean function
                    self.columns_formatter(df)
                    break 
            except TypeError:
                continue
        print("Re-exécution terminée.")
        return df
    
    def return_csv(self, df, path):
        path_split = path.split('/')
        dataset = path_split[2]
        table = path_split[3].split('.')[0]
        table = table.replace(" ", "_")
        os.makedirs(f'data/datasets/prep_datasets/{path_split[2]}', exist_ok=True)
        df.to_csv(f'data/datasets/prep_datasets/{dataset}/{table}.csv', index=False, sep=";")
    
        

