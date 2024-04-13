import pandas as pd
from .connectors import GoogleConnector
from pandas.errors import ParserError
from google.oauth2 import service_account
from google.cloud import storage
import zipfile
import tempfile
import os
import io
from colorama import Fore, Style
import re
from unidecode import unidecode
from IPython.display import display

class PrepFilesBQ:

    def __init__(self, paths=None, zip_file=None):
        self.paths = paths
        self.zip_file = zip_file

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
        else:
            print('More less 1 percent of rows skipped, file is okay')
        
        return df

    def correct_shape(self, file, df):
        try:
            if df.shape[1] == 1:
                print('columns shape is 1, csv read with ;')
                print(type(file))
                if 'zipfile.ZipExtFile' in str(type(file)):
                    file.seek(0)
                df = pd.read_csv(file, sep=';')
                if df.shape[1] == 1:
                    print('try to find headers in 2nd row')
                    if 'zipfile.ZipExtFile' in str(type(file)):
                        file.seek(0)
                    df = pd.read_csv(file, sep=';', skiprows=1)
                else:
                    df = df
            elif 'unnamed' in str(df.columns[1]).lower():
                print('try to find headers in 2nd row')
                df.columns = df.iloc[0]
                df = df[1:]
        except ParserError as e:
            print(f"{Fore.RED}Exception type: {type(e).__name__}{Style.RESET_ALL}")
            print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}, return None")
            print('cant correct shape')
            df = None
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
        else:
            print('More rows than columns, no need to transpose')

        return df

    def open_csv_file(self, path, file):
        if file is None:
            file = path
        print('file:', file)
        print('path:', path)
        try:
            df = pd.read_csv(file)
        except ParserError as e:
            print(f"{Fore.RED}Exception type (first attempt): {type(e).__name__}{Style.RESET_ALL}")
            print(e)
            print('trying to open csv with sep = ";"')
            try:
                df = pd.read_csv(file, sep=';')
            except Exception as e:
                print(f"{Fore.RED}Exception type (second attempt): {type(e).__name__}{Style.RESET_ALL}")
                print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}")
                print('Unable to open CSV file')
                df = None

        if df is not None:
            print(df.shape)
            df = self.correct_shape(file, df)
            return df
        else:
            return df

    def open_excel_file(self, path, file):
        if file is None:
            file = path
        df = pd.read_excel(file)
        print(df.shape)
        df = self.correct_shape(file, df)
        return df
    
    def columns_formatter(self, df):
        new_columns = []
        for index, column in enumerate(df.columns):
            try:
                column = str(column)
                column = re.sub(r'[!\"$()\*\.,\/;?\@\[\]\\^`{}\~]', '', column)
                column = column.strip().lower().replace(" ", "_")
                column = unidecode(column)
                column = column.replace("\n", "_")
                column = column.replace("'", "")
                column = column.replace("-", "")
                column = column.replace("&", 'and')
                column = column.replace('<', "").replace('>', "")
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
                    self.columns_formatter(df)
                    break 
            except TypeError:
                continue
        print("Re-exécution terminée.")
        return df
    
    def rename_duplicate_columns(self, df):
        column_count = {}
        new_columns = []

        for column in df.columns:
            if column in column_count:
                column_count[column] += 1
                new_column = f"{column}_{column_count[column]}"
            else:
                column_count[column] = 1
                new_column = column

            new_columns.append(new_column)
        df.columns = new_columns
        return df


    def open_df(self, path, file=None):
        if file is None:
            file = path

        print(file)
        print(path)

        df = None

        if path.endswith('.csv'):
            print('.csv found')
            df = self.open_csv_file(path, file)
        elif path.endswith('.xlsx'):
            print('.xsxl found')
            df = self.open_excel_file(path, file)
        elif "." not in path:
            try:
                try: 
                    print('try to read as csv')
                    df = self.open_csv_file(path, file)
                except ParserError as e:
                    print(f"{Fore.RED}Exception type: {type(e).__name__}{Style.RESET_ALL}")
                    print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}")
                    print('try to read as excel')
                    df = self.open_excel_file(path, file)
                    print(df.shape)
                except UnicodeError as e:
                    print(f"{Fore.RED}Exception type: {type(e).__name__}{Style.RESET_ALL}")
                    print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}")
                    print('try to read as excel')
                    df = self.open_excel_file(path, file)
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
            display(df)
            if df is not None:
                df = self.transposed(df)
                df = self.drop_empty_columns(df)
                df = self.columns_formatter(df)
                df = self.check_column_clean(df)
                df = self.rename_duplicate_columns(df)
                self.return_csv(df, path)
                print(Fore.GREEN + f"{path} processed successfully!" + Style.RESET_ALL)
                print("---------------------------------------------------")
            else:
                print(Fore.RED + f"{path} not processed!" + Style.RESET_ALL)
                print("---------------------------------------------------")
    
    def process_zip_file(self, zip_file):
        file_list = [file for file in zip_file.namelist()]
        filtered_list = list(filter(lambda x: not x.endswith('/'), file_list))
        output_zip = io.BytesIO()

        with zipfile.ZipFile(output_zip, 'w') as temp_zip:
            for path in filtered_list:
                print(Fore.GREEN + 'current:', path + Style.RESET_ALL)
                with zip_file.open(path) as file:
                    print("---------------------------------------------------")
                    print(Fore.GREEN + path + Style.RESET_ALL)
                    if path == '.DS_Store':
                        print(Fore.RED + f"{path} not processed!" + Style.RESET_ALL)
                        print("---------------------------------------------------")
                        continue
                    df = self.open_df(path, file)
                    print('this is df')
                    if df is not None:
                        df = self.transposed(df)
                        df = self.drop_empty_columns(df)
                        df = self.columns_formatter(df)
                        df = self.check_column_clean(df)
                        df = self.rename_duplicate_columns(df)

                        csv_output = io.StringIO()
                        df.to_csv(csv_output, index=False, sep=";")
                        csv_output.seek(0)

                        # Ajouter le CSV à l'archive temporaire
                        temp_zip.writestr(path, csv_output.getvalue())

                        print(Fore.GREEN + f"{path} processed successfully!" + Style.RESET_ALL)
                        print("---------------------------------------------------")
                    else:
                        print(Fore.RED + f"{path} not processed!" + Style.RESET_ALL)
                        print("---------------------------------------------------")

        # Retourner le fichier zip temporaire en mémoire
        output_zip.seek(0)
        output_zip = self.replace_char_in_filename(output_zip)
        output_zip.seek(0)
        return output_zip
    
    def replace_char_in_filename(self, zip_file):
        output_zip = io.BytesIO()
        with zipfile.ZipFile(zip_file, 'r') as zip_file:
            with zipfile.ZipFile(output_zip, 'w') as new_zip:
                for zip_entry in zip_file.infolist():
                    # Remplacer les espaces par des underscores dans le nom du fichier
                    new_filename = zip_entry.filename.replace(' ', '_').replace('-', '_').replace(".", "_")
                    new_filename = new_filename + ".csv"
                    # Ajouter le fichier au nouveau zip avec le nouveau nom
                    new_zip.writestr(new_filename, zip_file.read(zip_entry))

        output_zip.seek(0)
        return output_zip

                    
class PrepDataCnilBQ(PrepFilesBQ):

    def __init__(self, paths):
        super().__init__(paths)

    def transposed(self, df):
        if df.shape[1] > df.shape[0]:
            print('More columns than rows, need to transpose')
            df = df.transpose()
            df.columns = df.iloc[0]
            df = df[1:]  
            column1 = df.columns.name
            df = df.reset_index()
            df = df.rename(columns={df.columns[0]: column1})
        elif df.shape[1] <= df.shape[0] and df.columns[0].lower() == 'année' and str(df.columns[1]).isdigit() and len(str(df.columns[1])) == 4:
            print('Values for years in first column, need to transpose')
            df = df.transpose()
            df.columns = df.iloc[0]
            df = df[1:] 
            column1 = df.columns.name
            df = df.reset_index()
            df = df.rename(columns={df.columns[0]: column1})
        else:
            print('More rows than columns, no need to transpose')
        
        return df
    
    def columns_formatter(self, df):
        new_columns = []
        for index, column in enumerate(df.columns):
            try:
                column = str(column)
                column = re.sub(r'[!\"$()\*\.,\/;?\@\[\]\\^`{}\~]', '', column)
                column = column.strip().lower().replace(" ", "_")
                column = unidecode(column)
                column = column.replace("\n", "_")
                column = column.replace("'", "")
                column = column.replace("-", "")
                column = column.replace("&", 'and')
                column = column.replace('https:edpbeuropaeuaboutedpbboardmembers_fr', "").replace('https:wwwafapdporglafapdpmembres', "")
                column = column.replace('<', "").replace('>', "")
                column = column[:200]
                new_columns.append(column)
            except AttributeError:
                new_columns.append(column)
                continue
    
        df.columns = new_columns
        return df