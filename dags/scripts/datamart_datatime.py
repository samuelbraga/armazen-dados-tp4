import os
import sys
import pandas as pd

from scripts.utils.csv import Csv

def create_datetime_datamart(bucket_name, file_key):
    
    file_name = Csv.get_file_name_from_file_key(file_key, prefix="")
    df = Csv.read_local_csv_to_dataframe(file_name, nrows=9999)
    df.reset_index()

    datamart = pd.DataFrame()
    
    for index, row in df.iterrows():
        datamart = set_datamart_datetime_pickup(datamart, df, index)
        datamart = set_datamart_datetime_dropoff(datamart, df, index)
    
    datamart.reset_index()

    file_name = Csv.get_file_name_from_file_key(file_key, prefix='datetime_')
    file_name = file_name.replace("normalized_", "")
    Csv.write_local_csv_from_dataframe(datamart, file_name)
    Csv.upload_csv_to_s3(bucket_name, file_name, s3_folder='datamart/datetime/')

def set_datamart_datetime_pickup(datamart, df, index):
    new_rol = {
        'id': df.at[index, 'datetime_pickup_id'],
        'day': df.at[index, 'day_pickup'],
        'month': df.at[index, 'month_pickup'],
        'year': df.at[index, 'year_pickup'],
        'hour': df.at[index, 'hour_pickup'],
        'shift': df.at[index, 'shift_pickup'],
        'day_of_week': df.at[index, 'day_of_week_pickup']
    }

    datamart = datamart.append(new_rol, ignore_index=True)
    
    return datamart

def set_datamart_datetime_dropoff(datamart, df, index):
    new_rol = {
        'id': df.at[index, 'datetime_dropoff_id'],
        'day': df.at[index, 'day_dropoff'],
        'month': df.at[index, 'month_dropoff'],
        'year': df.at[index, 'year_dropoff'],
        'hour': df.at[index, 'hour_dropoff'],
        'shift': df.at[index, 'shift_dropoff'],
        'day_of_week': df.at[index, 'day_of_week_dropoff']
    }

    datamart = datamart.append(new_rol, ignore_index=True)
    
    return datamart
