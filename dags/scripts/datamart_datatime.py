import os
import sys
import pandas as pd

from scripts.utils.csv import Csv

def create_datamart(bucket_name, file_key):
    
    file_name = Csv.get_file_name_from_file_key(file_key, prefix="")
    df = Csv.read_local_csv_to_dataframe(file_name, nrows=9999)
    df.reset_index()

    datamart = pd.DataFrame()
    
    for index, row in df.iterrows():
        datamart = set_datamart_datetime(datamart, df, index)
    
    datamart.reset_index()

    file_name = Csv.get_file_name_from_file_key(file_key, prefix='datetime_')
    file_name = file_name.replace("normalized_", "")
    Csv.write_local_csv_from_dataframe(datamart, file_name)
    Csv.upload_csv_to_s3(bucket_name, file_name, s3_folder='datamart/datetime/')

def set_datamart_datetime(datamart, df, index):
    new_rol = {
        'id': df.at[index, 'datetime_id'],
        'day': df.at[index, 'day'],
        'month': df.at[index, 'month'],
        'year': df.at[index, 'year'],
        'hour': df.at[index, 'hour'],
        'shift': df.at[index, 'shift'],
        'day_of_week': df.at[index, 'day_of_week']
    }

    datamart = datamart.append(new_rol, ignore_index=True)
    
    return datamart
