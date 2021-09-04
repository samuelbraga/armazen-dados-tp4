import os
import sys
import pandas as pd

from scripts.utils.s3 import get_file

def get_data_frame(bucket_name, file_key):
    csv = get_file(bucket_name, file_key)
    body = csv['Body']
    return pd.read_csv(body)