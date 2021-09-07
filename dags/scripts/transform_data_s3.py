import os
import sys
import pandas as pd
from dateutil.parser import parse
from datetime import datetime, timedelta

from scripts.utils.s3 import get_file, upload_file

def transform_data(bucket_name, file_key):
    df = get_data_frame(bucket_name, file_key)
    reset_index(df)
    create_columns(df)
    for index, row in df.iterrows():
        broke_date(df, index)
    write_csv(df)
    upload_csv(bucket_name)
    
def get_data_frame(bucket_name, file_key):
    csv = get_file(bucket_name, file_key)
    body = csv['Body']
    return pd.read_csv(body, nrows=9999)

def reset_index(df):
    df.reset_index()

def create_columns(df):
    df["day"] = ""
    df["month"] = ""
    df["year"] = ""
    df["hour"] = ""
    df["day_of_week"] = ""
    df["shift"] = ""

def broke_date(df, index):
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    pickup_date = parse(df.at[index, 'tpep_pickup_datetime'])
    
    df.at[index, 'day'] = pickup_date.day
    df.at[index, 'month'] = pickup_date.month
    df.at[index, 'year'] = pickup_date.year
    df.at[index, 'hour'] = pickup_date.hour
    df.at[index, 'day_of_week'] = days[pickup_date.weekday()]

    if pickup_date.hour >= 4 and pickup_date.hour < 12:
        df.at[index, 'shift'] = "MORNING"
    if pickup_date.hour >= 12 and pickup_date.hour < 18:
        df.at[index, 'shift'] = "AFTERNOON"
    if pickup_date.hour >= 18 or pickup_date.hour < 4:
        df.at[index, 'shift'] = "NIGHT" 

def write_csv(data, file_path='./dags/tmp/'):
    file_name = 'teste' + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')+'.csv'
    file_path = file_path + file_name
    data.to_csv(file_path, index_label='id')

def upload_csv(bucket_name, file_path='./dags/tmp/'):
    file_name = 'teste' + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')+'.csv'
    file_path = file_path + file_name
    file_key = "normalized/" + file_name
    upload_file(bucket_name, file_path, file_key)