import os
import sys
import pandas as pd
from dateutil.parser import parse
from datetime import datetime, timedelta

from scripts.utils.s3 import get_file

def transform_data(bucket_name, file_key):
    df = get_data_frame(bucket_name, file_key)
    create_columns(df)
    for index, row in df.iterrows():
        broke_date(df, index)
    write_csv(df)
    
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

def broke_date(df, index):
    pickup_date = parse(df.at[index, 'tpep_pickup_datetime'])
    
    df.at[index, 'day'] = pickup_date.day
    df.at[index, 'month'] = pickup_date.month
    df.at[index, 'year'] = pickup_date.year
    df.at[index, 'hour'] = pickup_date.hour

def write_csv(data, filepath='./dags/tmp/'):
    filename = 'teste' + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')+'.csv'
    filepath = filepath + filename
    data.to_csv(filepath, index_label='id')