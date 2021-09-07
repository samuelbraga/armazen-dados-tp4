import os
import sys
import pandas as pd
from dateutil.parser import parse
from datetime import datetime, timedelta
from scripts.utils.holidays import Holidays
from scripts.utils.location import Location

from scripts.utils.s3 import get_file
FILE_PATH = './dags/tmp/'

def transform_data(bucket_name, file_key):
    df = get_data_frame(bucket_name, file_key)
    create_columns(df)
    try:
        year = get_year_from_file_key(file_key)
    except ValueError:
        raise Exception('unable to get year')
    
    holiday_data = Holidays(year)
    for index, row in df.iterrows():
        broke_date(df, index, holiday_data)
    write_csv(df)

def parse_location_dimension(file_path):
    df = pd.read_csv(file_path)
    location_parser = Location(df)
    location_parser.parse_dimension()
    
    location_parser.dataframe.to_csv(FILE_PATH+'location.csv', index_label='id')
    
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
    df["is_holiday"] = ""

def broke_date(df, index, holiday_data):
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    pickup_date = parse(df.at[index, 'tpep_pickup_datetime'])
    
    df.at[index, 'day'] = pickup_date.day
    df.at[index, 'month'] = pickup_date.month
    df.at[index, 'year'] = pickup_date.year
    df.at[index, 'hour'] = pickup_date.hour
    df.at[index, 'day_of_week'] = days[pickup_date.weekday()]
    df.at[index, 'is_holiday'] = holiday_data.is_holiday(pickup_date)
    if pickup_date.hour >= 4 and pickup_date.hour < 12:
        df.at[index, 'shift'] = "MORNING"
    if pickup_date.hour >= 12 and pickup_date.hour < 18:
        df.at[index, 'shift'] = "AFTERNOON"
    if pickup_date.hour >= 18 and pickup_date.hour < 4:
        df.at[index, 'shift'] = "NIGHT" 

def write_csv(data, filepath='./dags/tmp/'):
    filename = 'teste' + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')+'.csv'
    filepath = filepath + filename
    data.to_csv(filepath, index_label='id')

def get_year_from_file_key(file_key):
    file_name_parts = file_key.split('_')
    year_month = file_name_parts[-1].split('-')
    year = int(year_month[0])
    return year