import pandas as pd
from pandas.errors import ParserError
import zipfile
import io
import os
import re
from unidecode import unidecode
from colorama import Fore, Style
import csv

class PrepFilesBQ:
    """
    Class to preprocess files before loading them into BigQuery.
    The main difficulty is to handle the different formats of the files.

    Attributes:
    - paths: List of file paths to be processed.
    - zip_file: Optional. Zip file containing files to be processed.
    """

    def __init__(self, paths=None):
        """
        Initialize PrepFilesBQ object.

        Args:
        - paths: Optinnal. List of file paths.
        - zip_file: Optional. Zip file containing files.
        """
        self.paths = paths
    
    def process_zip_file(self, zip_file):
        """
        Processes a zip file containing multiple files.

        Args:
        - zip_file: Zip file object.

        Returns:
        - BytesIO: BytesIO object containing the processed zip file.
        """

        # Get list of files in zip
        # Namelist will returns a list of all the files and directories in the archive
        file_list = [file for file in zip_file.namelist()]
        # Filter out directories
        filtered_list = list(filter(lambda x: not x.endswith('/'), file_list))
        output_zip = io.BytesIO()

        self.files_not_processed = []
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

                        # Add CSV to temporary archive
                        temp_zip.writestr(path, csv_output.getvalue())

                        print(Fore.GREEN + f"{path} processed successfully!" + Style.RESET_ALL)
                        print("---------------------------------------------------")
                    else:
                        print(Fore.RED + f"{path} not processed!" + Style.RESET_ALL)
                        self.files_not_processed.append(path)
                        print("---------------------------------------------------")

        output_zip.seek(0)
        print('this is the list of files not processed:', self.files_not_processed)
        print('To access the list of files not processed, use the attribute instance.files_not_processed')
        return output_zip
    
    def process_zip_io_file(self, zip_file_io, filter_on=None):
        """
        Processes a zip file containing multiple files.

        Args:
        - zip_file: Zip file object.

        Returns:
        - BytesIO: BytesIO object containing the processed zip file.
        """

        # Get list of files in zip
        # Namelist will returns a list of all the files and directories in the archive
        with zipfile.ZipFile(zip_file_io, 'r') as zip_file:
            file_list = [file for file in zip_file.namelist()]

            if filter_on is not None:
                filtered_list = list(filter(lambda x: not x.endswith('/'), file_list))
                filtered_list = list(filter(lambda x: filter_on in x, filtered_list))
            else:
                filtered_list = list(filter(lambda x: not x.endswith('/'), file_list))

            self.filtered_list = filtered_list

            output_zip = io.BytesIO()

            self.files_not_processed = []
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
                        
                        if path.endswith('.csv'):
                            try:
                                with open(file, 'r') as read_obj:
                                    csv_reader = csv.reader(read_obj)
                                    list_of_csv = list(csv_reader) 
                                    if len(list_of_csv) > 1:
                                        print('not empty csv')
                                    else:
                                        print('empty csv')
                                        df = None
                            except Exception as e:
                                print(f"{Fore.RED}Exception type: {type(e).__name__}{Style.RESET_ALL}")
                                print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}")
                                print('cant read as csv')
                                df = None 
                            
                        df = self.open_df(path, file)
                        print('this is df')
                        if df is not None:
                            df = self.transposed(df)
                            df = self.drop_empty_columns(df)
                            df = self.columns_formatter(df)
                            df = self.check_column_clean(df)
                            df = self.rename_duplicate_columns(df)

                            temp_zip.writestr(path, df.to_csv(index=False, sep=";"))
                            print(Fore.GREEN + f"{path} processed successfully!" + Style.RESET_ALL)
                            print("---------------------------------------------------")
                        else:
                            print(Fore.RED + f"{path} not processed!" + Style.RESET_ALL)
                            self.files_not_processed.append(path)
                            print("---------------------------------------------------")
            
        output_zip.seek(0)
        print('this is the list of files not processed:', self.files_not_processed)
        print('To access the list of files not processed, use the attribute instance.files_not_processed')
        return output_zip
    
    def replace_char_in_filename(self, zip_file):
        """
        Replaces characters in filenames of the zip file.

        Args:
        - zip_file: Zip file object.

        Returns:
        - BytesIO: BytesIO object containing the processed zip file.
        """
        output_zip = io.BytesIO()
        with zipfile.ZipFile(zip_file, 'r') as zip_file:
            with zipfile.ZipFile(output_zip, 'w') as new_zip:
                for zip_entry in zip_file.infolist():
                    # Replace spaces with underscores in filename
                    new_filename = zip_entry.filename.replace(' ', '_').replace('-', '_').replace(".", "_").replace("(", "_").replace(")", "").replace(",", "")
                    new_filename = new_filename + ".csv"
                    # Add file to new zip with new name
                    new_zip.writestr(new_filename, zip_file.read(zip_entry))

        output_zip.seek(0)
        return output_zip

    def process_dfs(self, zip_file_io, filter_on=None):
        """
        Processes a zip file containing multiple files.

        Args:
        - zip_file: Zip file object.

        Returns:
        - BytesIO: BytesIO object containing the processed zip file.
        """

        # Get list of files in zip
        # Namelist will returns a list of all the files and directories in the archive
        with zipfile.ZipFile(zip_file_io, 'r') as zip_file:
            file_list = [file for file in zip_file.namelist()]

            if filter_on is not None:
                filtered_list = list(filter(lambda x: not x.endswith('/'), file_list))
                filtered_list = list(filter(lambda x: filter_on in x, filtered_list))
            else:
                filtered_list = list(filter(lambda x: not x.endswith('/'), file_list))

            self.filtered_list = filtered_list

        # Filter out directories

            dfs = []
            self.files_not_processed = []
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

                        dict_df = {
                            'path': self.replace_char_in_df_names(path),
                            'df': df
                        }
                        dfs.append(dict_df)

                        print(Fore.GREEN + f"{path} processed successfully!" + Style.RESET_ALL)
                        print("---------------------------------------------------")
                    else:
                        print(Fore.RED + f"{path} not processed!" + Style.RESET_ALL)
                        self.files_not_processed.append(path)
                        print("---------------------------------------------------")
        return dfs
    
    def replace_char_in_df_names(self, path):
        """
        Replaces characters in filenames associated with df.

        Args:
        - path: a filename associated with a df.

        Returns:
        - new_filename: a new filename adapted for bigquery.
        """
        new_filename = path.replace(' ', '_').replace('-', '_').replace(".", "_").replace("(", "_").replace(")", "").replace(",", "")
        new_filename = new_filename + ".csv"
        return new_filename
    
    def save_dfs_to_zip(self, dfs):
        """
        Save a DataFrame to a CSV file within a zip archive.

        Parameters:
            dfs (pandas.DataFrame): The DataFrame to be saved.

        Returns:
            None
        """
        # Create a zip file
        zip_output = io.BytesIO()
        with zipfile.ZipFile(zip_output, 'w') as zipf:
                for file in dfs:
                    df = file['df']
                    filename = file['path']
                    # Write DataFrame to a CSV file in memory
                    csv_buffer = df.to_csv(index=False, sep=';')

                    # Add the CSV file to the zip archive
                    zipf.writestr(filename, csv_buffer)

        zip_output.seek(0)
        return zip_output
    
    def open_df(self, path, file=None):
        """
        Opens and reads a file.

        Args:
        - path: File path.
        - file: Optional. File object or path.

        Returns:
        - DataFrame: DataFrame containing file data.
        """
        if file is None:
            file = path

        print(file)
        print(path)

        df = None

        if path.endswith('.csv'):
            print('.csv found')
            df = self.open_csv_file(file=file)
        elif path.endswith('.xlsx'):
            print('.xsxl found')
            df = self.open_excel_file(file=file)
        elif "." not in path:
            try:
                print('try to read as csv')
                df = self.open_csv_file(file=file)
            except ParserError as e:
                print(f"{Fore.RED}Exception type: {type(e).__name__}{Style.RESET_ALL}")
                print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}")
                print('try to read as excel')
                df = self.open_excel_file(file=file)
                print(df.shape)
            except UnicodeError as e:
                print(f"{Fore.RED}Exception type: {type(e).__name__}{Style.RESET_ALL}")
                print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}")
                print('try to read as excel')
                df = self.open_excel_file(file=file)
                print(df.shape)
            except Exception as e:
                print(f"{Fore.RED}Exception type: {type(e).__name__}{Style.RESET_ALL}")
                print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}")
                print('cant read as df')
                df = None

        print('opened df, return from open_df')
        return df
    
    def open_csv_file(self, file):
        """
        Opens and reads a CSV file.


        Args:
        - path: File path.
        - file: File object or path.

        Returns:
        - DataFrame: DataFrame containing CSV data.
        """
        print('file:', file)


        try:
            df = pd.read_csv(file)
        except UnicodeDecodeError as e:
            if 'zipfile.ZipExtFile' in str(type(file)):
                    file.seek(0)
            print(f"{Fore.RED}Exception type (first attempt): {type(e).__name__}{Style.RESET_ALL}")
            print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}")
            df = pd.read_csv(file, sep=';', encoding='latin1')
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

    def open_excel_file(self, file):
        """
        Opens and reads an Excel file.

        Args:
        - path: File path.
        - file: File object or path.

        Returns:
        - DataFrame: DataFrame containing Excel data.
        """
        if file is None:
            file = path
        df = pd.read_excel(file)
        print(df.shape)
        df = self.correct_shape(file, df)
        return df

    def verify_error_onbadlines(self, path, df):
        """
        Verifies and handles errors caused by bad lines in the CSV file.
        If more than 1% of rows are skipped, the file is considered bad. It will be skipped.

        Args:
        - path: File path.
        - df: DataFrame containing the CSV data.

        Returns:
        - DataFrame: Processed DataFrame.
        """
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
        """
        Corrects the shape of the DataFrame if needed.
        A csv file can be read with ',' separator and give a wrong shape.
        This methods can correct the shape of the DataFrame with the good separator or by skipping the first row.

        Args:
        - file: File object or path.
        - df: DataFrame to be corrected.

        Returns:
        - DataFrame: Corrected DataFrame.
        """
        try:
            # Handling if CSV is read with ';' separator
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
            # Handling if column names are unnamed
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
        """
        Transposes the DataFrame if the number of columns exceeds the number of rows.
        A DataFrame with more columns than rows is transposed.

        Args:
        - df: DataFrame to be transposed.

        Returns:
        - DataFrame: Transposed DataFrame.
        """
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

    
    
    def columns_formatter(self, df):
        """
        Formats column names of the DataFrame to be clean and readable by BigQuery.

        Args:
        - df: DataFrame.

        Returns:
        - DataFrame: DataFrame with formatted column names.
        """
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
        """
        Checks if column names are clean and reformats them if needed.

        Args:
        - df: DataFrame.

        Returns:
        - DataFrame: DataFrame with cleaned column names.
        """
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
        print("Re-execution completed.")
        return df
    
    def rename_duplicate_columns(self, df):
        """
        Renames duplicate columns in the DataFrame.

        Args:
        - df: DataFrame.

        Returns:
        - DataFrame: DataFrame with renamed duplicate columns.
        """
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

    def drop_empty_columns(self, df):
        """
        Drops empty columns from the DataFrame.

        Args:
        - df: DataFrame.

        Returns:
        - DataFrame: DataFrame with empty columns dropped.
        """
        if df is not None:
            df = df.dropna(axis=1, how='all')
            return df
        else:
            return df
    
    def return_csv(self, df, path):
        """
        Saves DataFrame to CSV file.

        Args:
        - df: DataFrame.
        - path: File path to save CSV file.
        """
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
        table = table.replace("(", "_").replace(")", "_")
        os.makedirs(f'data/prep_datasets/{path_split[2]}', exist_ok=True)
        df.to_csv(f'data/prep_datasets/{dataset}/{table}_{extension}.csv', index=False, sep=";")
    
    def process_all_files(self):
        """
        Processes all files in the paths attribute.
        """
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
