import os
import sys
import pandas as pd

from scripts.utils.s3 import get_file

def transform_data(bucket_name, file_key):
    df = get_data_frame(bucket_name, file_key)
    for index, row in df.iterrows():
        print(row['VendorID'])
    
def get_data_frame(bucket_name, file_key):
    csv = get_file(bucket_name, file_key)
    body = csv['Body']
    return pd.read_csv(body, nrows=9999)