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

DAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

def transform_data(bucket_name, file_key):
    df = Csv.download_csv_from_s3_to_dataframe(bucket_name, file_key, nrows=9999)
    
    df.reset_index()
    
    try:
        year = get_year_from_file_key(file_key)
    except ValueError:
        raise Exception('unable to get year')
    holiday_data = Holidays(year)

    for index, row in df.iterrows():
        get_trip_duration(df, index)
        broke_date_pickup(df, index, holiday_data)
        broke_date_dropoff(df, index, holiday_data)
        set_datamart_ids(df, index)

    file_name = Csv.get_file_name_from_file_key(file_key)
    Csv.write_local_csv_from_dataframe(df, file_name, index_label='id')
    Csv.upload_csv_to_s3(bucket_name, file_name)

def get_trip_duration(df, index):
    pickup_date = parse(df.at[index, 'tpep_pickup_datetime'])
    dropoff_date = parse(df.at[index, 'tpep_dropoff_datetime'])
    trip_duration = dropoff_date - pickup_date
    df.at[index, 'trip_duration'] = trip_duration.seconds / 60

def broke_date_pickup(df, index, holiday_data):
    pickup_date = parse(df.at[index, 'tpep_pickup_datetime'])
    
    df.at[index, 'day_pickup'] = pickup_date.day
    df.at[index, 'month_pickup'] = pickup_date.month
    df.at[index, 'year_pickup'] = pickup_date.year
    df.at[index, 'hour_pickup'] = pickup_date.hour
    df.at[index, 'day_of_week_pickup'] = DAYS[pickup_date.weekday()]
    df.at[index, 'is_holiday_pickup'] = holiday_data.is_holiday(pickup_date)
    if pickup_date.hour >= 4 and pickup_date.hour < 12:
        df.at[index, 'shift_pickup'] = "MORNING"
    if pickup_date.hour >= 12 and pickup_date.hour < 18:
        df.at[index, 'shift_pickup'] = "AFTERNOON"
    if pickup_date.hour >= 18 or pickup_date.hour < 4:
        df.at[index, 'shift_pickup'] = "NIGHT"

def broke_date_dropoff(df, index, holiday_data):
    dropoff_date = parse(df.at[index, 'tpep_dropoff_datetime'])
    
    df.at[index, 'day_dropoff'] = dropoff_date.day
    df.at[index, 'month_dropoff'] = dropoff_date.month
    df.at[index, 'year_dropoff'] = dropoff_date.year
    df.at[index, 'hour_dropoff'] = dropoff_date.hour
    df.at[index, 'day_of_week_dropoff'] = DAYS[dropoff_date.weekday()]
    df.at[index, 'is_holiday_dropoff'] = holiday_data.is_holiday(dropoff_date)
    if dropoff_date.hour >= 4 and dropoff_date.hour < 12:
        df.at[index, 'shift_dropoff'] = "MORNING"
    if dropoff_date.hour >= 12 and dropoff_date.hour < 18:
        df.at[index, 'shift_dropoff'] = "AFTERNOON"
    if dropoff_date.hour >= 18 or dropoff_date.hour < 4:
        df.at[index, 'shift_dropoff'] = "NIGHT"
    
def set_datamart_ids(df, index):
    df.at[index, 'datetime_pickup_id'] = uuid4()
    df.at[index, 'datetime_dropoff_id'] = uuid4()
    df.at[index, 'tip_id'] = uuid4()
    df.at[index, 'fare_id'] = uuid4()
    df.at[index, 'dropoff_location_id'] = uuid4()
    df.at[index, 'pickup_location_id'] = uuid4()
    df.at[index, 'payment_id'] = uuid4()

def get_year_from_file_key(file_key):
    file_name_parts = file_key.split('_')
    year_month = file_name_parts[-1].split('-')
    year = int(year_month[0])
    return year

def parse_location_dimension(file_path):
    df = pd.read_csv(file_path)
    location_parser = Location(df)
    location_parser.parse_dimension()
    
    location_parser.dataframe.to_csv('location.csv', index_label='id')
