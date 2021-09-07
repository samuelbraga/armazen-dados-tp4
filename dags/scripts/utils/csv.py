
import pandas as pd
from scripts.utils.s3 import get_file, upload_file
class Csv:
    FILE_PATH = './dags/tmp/'
    @staticmethod
    def read_local_csv_to_dataframe(file_name, nrows=9999):
        if not file_name.endswith('.csv'):
            file_name+='.csv'
        return pd.read_csv(Csv.FILE_PATH+file_name, nrows=nrows)
    
    @staticmethod
    def write_local_csv_from_dataframe(dataframe: pd.DataFrame,file_name,index_label=None):
        if not file_name.endswith('.csv'):
            file_name+='.csv'
        if index_label == None:
            dataframe.to_csv(Csv.FILE_PATH+file_name,index=False)
        if index_label:
            dataframe.to_csv(Csv.FILE_PATH+file_name,index_label=index_label)
    
    @staticmethod
    def download_csv_from_s3_to_dataframe(bucket_name, file_key, nrows=9999):
        csv = get_file(bucket_name, file_key)
        body = csv['Body']
        return pd.read_csv(body, nrows=nrows)
    
    @staticmethod
    def upload_dataframe_to_s3(dataframe, bucket_name, file_name, s3_folder='normalized/'):
        Csv.write_local_csv_from_dataframe(dataframe, file_name)
        Csv.upload_csv_to_s3(bucket_name, file_name, s3_folder)

    @staticmethod
    def upload_csv_to_s3(bucket_name, file_name, s3_folder='normalized/'):
        if not file_name.endswith('.csv'):
            file_name+='.csv'
        file_path = Csv.FILE_PATH + file_name
        file_key = s3_folder + file_name
        upload_file(bucket_name, file_path, file_key)
    
    @staticmethod
    def get_file_name_from_file_key(file_key, prefix="normalized_"):
        file_key_parts = file_key.split('/')
        file_name = file_key_parts[-1]
        file_name = prefix+file_name
        return file_name