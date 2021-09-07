import os
import sys
import pandas as pd
from uuid import uuid4
from dateutil.parser import parse
from datetime import datetime, timedelta
from scripts.utils.holidays import Holidays
from scripts.utils.location import Location


from scripts.utils.csv import Csv
from scripts.utils.s3 import get_file, upload_file
from scripts.utils.s3 import get_file
FILE_PATH = './dags/tmp/'

def transform_data(bucket_name, file_key):
    df = Csv.download_csv_from_s3_to_dataframe(bucket_name, file_key, nrows=9999)
    reset_index(df)
    try:
        year = get_year_from_file_key(file_key)
    except ValueError:
        raise Exception('unable to get year')
    
    holiday_data = Holidays(year)
    for index, row in df.iterrows():
        broke_date(df, index, holiday_data)
    file_name = Csv.get_file_name_from_file_key(file_key)
    Csv.write_local_csv_from_dataframe(df, file_name, index_label='id')
    #Csv.upload_csv_to_s3(bucket_name, file_name)

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
    if pickup_date.hour >= 18 or pickup_date.hour < 4:
        df.at[index, 'shift'] = "NIGHT"
    
    df.at[index, 'datetime_id'] = uuid4()
    df.at[index, 'tip_id'] = uuid4()
    df.at[index, 'local_id'] = uuid4()
    df.at[index, 'payment_id'] = uuid4()

def write_csv(data, file_path='./dags/tmp/'):
    file_name = 'teste' + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')+'.csv'
    file_path = file_path + file_name
    data.to_csv(file_path, index_label='id')

def upload_csv(bucket_name, file_path='./dags/tmp/'):
    file_name = 'teste' + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')+'.csv'
    file_path = file_path + file_name
    file_key = "normalized/" + file_name
    upload_file(bucket_name, file_path, file_key)

def get_year_from_file_key(file_key):
    file_name_parts = file_key.split('_')
    year_month = file_name_parts[-1].split('-')
    year = int(year_month[0])
    return year
