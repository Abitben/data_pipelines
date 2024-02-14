import pandas as pd
from pandas.errors import ParserError
from google.oauth2 import service_account
from google.cloud import storage
import zipfile
import os
import io
from colorama import Fore, Style
import re
from unidecode import unidecode
from IPython.display import display



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

class PrepFilesBQ:

    def __init__(self, paths):
        self.paths = paths

    def verify_error_onbadlines(self, path, df):
        with open(path) as f:
            len_csv = sum(1 for line in f)

        print(len_csv)
        print(df.shape)
        number_of_skipped_rows = len_csv - df.shape[0]
        print('number_of_skipped_rows:' , number_of_skipped_rows)
        errors_imp = number_of_skipped_rows/len_csv * 100
        if errors_imp > 1:
            print('More than 1 percent of rows skipped, file is not good')
            df = None
            return df
        else:
            print('More less 1 percent of rows skipped, file is okay')
            return df

    def correct_shape(self, path, df):
        try:
            if df.shape[1] == 1:
                print('columns shape is 1, csv read with ;')
                df = pd.read_csv(path, sep=';')
                if df.shape[1] == 1:
                    print('try to find headers in 2nd row')
                    df = pd.read_csv(path, sep=';', skiprows=1)
                    return df
                else:
                    return df
            elif 'unnamed' in str(df.columns[1]).lower():
                print('try to find headers in 2nd row')
                df.columns = df.iloc[0]
                df = df[1:]
                return df
            else:
                return df
        except ParserError as e:
            print(f"{Fore.RED}Exception type: {type(e).__name__}{Style.RESET_ALL}")
            print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}, return None")
            print('cant correct shape')
            df = None
            return df
        except Exception as e:
            print(f"{Fore.RED}Exception type: {type(e).__name__}{Style.RESET_ALL}")
            print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}, return None")
            df = None
            return df

    def transposed(self, df):
        if df.shape[1] > df.shape[0]:
            print('More columns than rows, need to transpose')
            df = df.transpose()
            df.columns = df.iloc[0]
            df = df[1:]  
            column1 = df.columns.name
            df = df.reset_index()
            df = df.rename(columns={df.columns[0]: column1})
            return df
        else:
            print('More rows than columns, no need to transpose')
            return df

    def open_csv_file(self, path):
        try:
            df = pd.read_csv(path)
        except ParserError as e:
            print(f"{Fore.RED}Exception type (first attempt): {type(e).__name__}{Style.RESET_ALL}")
            print(e)
            print('trying to open csv with sep = ";"')
            try:
                df = pd.read_csv(path, sep=';')
            except Exception as e:
                print(f"{Fore.RED}Exception type (second attempt): {type(e).__name__}{Style.RESET_ALL}")
                print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}")
                print('Unable to open CSV file')
                df = None

        if df is not None:
            print(df.shape)
            df = self.correct_shape(path, df)
            return df
        else:
            return df

    def open_excel_file(self, path):
        df = pd.read_excel(path)
        print(df.shape)
        df = self.correct_shape(path, df)
        return df
    
    def columns_formatter(self, df):
        new_columns = []
        for index, column in enumerate(df.columns):
            try:
                # Remplacement des caractères spéciaux
                column = str(column)
                
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


    def open_df(self, path):
        if path.endswith('.csv'):
            print('.csv found')
            df = self.open_csv_file(path)
        elif path.endswith('.xlsx'):
            print('.xsxl found')
            df = self.open_excel_file(path)
        elif "." not in path:
            try:
                try: 
                    print('try to read as csv')
                    df = self.open_csv_file(path)
                except ParserError as e:
                    print(f"{Fore.RED}Exception type: {type(e).__name__}{Style.RESET_ALL}")
                    print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}")
                    print('try to read as excel')
                    df = self.open_excel_file(path)
                    print(df.shape)
                except UnicodeError as e:
                    print(f"{Fore.RED}Exception type: {type(e).__name__}{Style.RESET_ALL}")
                    print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}")
                    print('try to read as excel')
                    df = self.open_excel_file(path)
                    print(df.shape)
            except Exception as e:
                print(f"{Fore.RED}Exception type: {type(e).__name__}{Style.RESET_ALL}")
                print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}")
                print('cant read as df')
                df = None

        print('opened df, return from open_df')
        return df
    
    def drop_empty_columns(self, df):
        if df is not None:
            df = df.dropna(axis=1, how='all')
            return df
        else:
            return df
    
    def return_csv(self, df, path):
        path_split = path.split('/')
        dataset = path_split[2]
        if '.' in path_split[3]:
            table_ext = path_split[3].split('.')
            table = table_ext[0]
            extension = table_ext[1]
        else:
            table = path_split[3]
            extension = ""
        table = table.replace(" ", "_")
        table = table.replace("-", "_")
        os.makedirs(f'data/prep_datasets/{path_split[2]}', exist_ok=True)
        df.to_csv(f'data/prep_datasets/{dataset}/{table}_{extension}.csv', index=False, sep=";")
    
    def process_all_files(self):
        for path in self.paths:
            print("---------------------------------------------------")
            print(Fore.GREEN + path + Style.RESET_ALL)
            df = self.open_df(path)
            print('this is df')
            if df is not None:
                # display(df.head(1))
                df = self.transposed(df)
                df = self.drop_empty_columns(df)
                df = self.columns_formatter(df)
                df = self.check_column_clean(df)
                self.return_csv(df, path)
                print(Fore.GREEN + f"{path} processed successfully!" + Style.RESET_ALL)
                print("---------------------------------------------------")
            else:
                print(Fore.RED + f"{path} not processed!" + Style.RESET_ALL)
                print("---------------------------------------------------")
    