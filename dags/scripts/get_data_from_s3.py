import os
import sys
import pandas as pd

from utils.s3 import get_file

def get_data_frame(bucket_name, filepath):
    csv = get_file(bucket_name, filepath)
    body = csv['Body']
    return pd.read_csv(body)