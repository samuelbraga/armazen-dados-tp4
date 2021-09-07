import os
import sys
import pandas as pd
from dateutil.parser import parse
from datetime import datetime, timedelta
from uuid import uuid4

from scripts.utils.csv import Csv
from scripts.utils.s3 import get_file, upload_file

def create_datamarts(bucket_name, file_key):
    df = get_data_frame(bucket_name, file_key)
    reset_index(df)
    create_columns(df)
    datamart_datetime = pd.DataFrame(columns = [
        'day',
        'month',
        'year',
        'hour',
        'shift',
        'day_of_week'],
        index=['id'])
    
    for index, row in df.iterrows():
        datamart_datetime = set_datamart_datetime(datamart_datetime, df, index)
    reset_index(df=datamart_datetime)
    file_name = Csv.get_file_name_from_file_key(file_key, prefix='table_datetime_')
    Csv.write_local_csv_from_dataframe(datamart_datetime, file_name)
    #Csv.upload_csv_to_s3(bucket_name, file_name)

    
def get_data_frame(bucket_name, file_key):
    csv = get_file(bucket_name, file_key)
    body = csv['Body']
    return pd.read_csv(body)

def reset_index(df):
    df.reset_index()

def create_columns(df):
    df['datetime_id'] = ""


def set_datamart_datetime(datamart_datetime, df, index):
    uuid = uuid4();
    new_rol = {
        'id': uuid,
        'day': df.at[index, 'day'],
        'month': df.at[index, 'month'],
        'year': df.at[index, 'year'],
        'hour': df.at[index, 'hour'],
        'shift': df.at[index, 'shift'],
        'day_of_week': df.at[index, 'day_of_week']
    }

    datamart_datetime = datamart_datetime.append(new_rol, ignore_index=True)
    df.at[index, 'datetime_id'] = uuid
    
    return datamart_datetime

def write_csv(data, file_path='./dags/tmp/'):
    file_name = 'teste' + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')+'.csv'
    file_path = file_path + file_name
    data.to_csv(file_path)

def write_csv11(data, file_path='./dags/tmp/'):
    file_name = 'teste11' + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')+'.csv'
    file_path = file_path + file_name
    data.to_csv(file_path)

# def upload_csv(bucket_name, file_path='./dags/tmp/'):
#     file_name = 'teste' + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')+'.csv'
#     file_path = file_path + file_name
#     file_key = "normalized/" + file_name
#     upload_file(bucket_name, file_path, file_key)