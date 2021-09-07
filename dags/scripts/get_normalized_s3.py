import os
import sys

from scripts.utils.csv import Csv

def get_file(bucket_name, file_key):
    df = Csv.download_csv_from_s3_to_dataframe(bucket_name, file_key, nrows=9999)
    file_name = Csv.get_file_name_from_file_key(file_key, prefix="")
    Csv.write_local_csv_from_dataframe(df, file_name)