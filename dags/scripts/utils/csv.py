from dags.scripts.transform_data_s3 import FILE_PATH
import pandas as pd
from scripts.utils.s3 import get_file, upload_file
class Csv:
    FILE_PATH = './dags/tmp/'
    @staticmethod
    def read_local_csv(file_name, nrows=9999):
        if not file_name.endswith('.csv'):
            file_name+='.csv'
        return pd.read_csv(Csv.FILE_PATH+file_name, nrows=nrows)
    
    @staticmethod
    def write_local_csv(dataframe: pd.DataFrame,file_name):
        if not file_name.endswith('.csv'):
            file_name+='.csv'
        dataframe.to_csv(Csv.FILE_PATH+file_name)
    
    @staticmethod
    def download_csv_from_s3(bucket_name, file_key, nrows=9999):
        csv = get_file(bucket_name, file_key)
        body = csv['Body']
        return pd.read_csv(body, nrows=nrows)
    
    @staticmethod
    def upload_csv_to_s3(bucket_name, file_name, s3_folder='normalized/'):
        if not file_name.endswith('.csv'):
            file_name+='.csv'
        file_path = Csv.FILE_PATH + file_name
        file_key = s3_folder + file_name
        upload_file(bucket_name, file_path, file_key)