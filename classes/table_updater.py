from .connectors import GoogleConnector
import time

class SQLTableUpdater(GoogleConnector):

    def __init__(self, credentials_path, project_id, dataset_id, table_id, column):
        super().__init__(credentials_path)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.complete_id = f"{project_id}.{dataset_id}.{table_id}"
        self.column = column

    def get_bq_table(self):
        self.df_bq = self.bq_client.query(f"SELECT * FROM `{self.complete_id}` ORDER BY {self.column} DESC").result().to_dataframe()
        return self.df_bq

    def compare_tables(self, df=None):
        pass
        