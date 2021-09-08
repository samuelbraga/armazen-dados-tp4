import os
import sys
import pandas as pd
from scripts.utils.file_keys import get_next_normalized_file_key

from scripts.utils.csv import Csv

def create_fare_datamart(bucket_name):
    file_key = get_next_normalized_file_key()
    if file_key == None:
        return
    
    file_name = Csv.get_file_name_from_file_key(file_key, prefix="")
    df = Csv.read_local_csv_to_dataframe(file_name, nrows=9999)
    df.reset_index()

    fare = pd.DataFrame()
    
    for index, row in df.iterrows():
        fare = set_datamart_fare(fare, df, index)
    
    fare.reset_index()

    file_name = Csv.get_file_name_from_file_key(file_key, prefix='fare_')
    file_name = file_name.replace("normalized_", "")
    Csv.write_local_csv_from_dataframe(fare, file_name)
    Csv.upload_csv_to_s3(bucket_name, file_name, s3_folder='datamart/fare/')

def set_datamart_fare(fare, df, index):
    new_rol = {
        'id': df.at[index, 'fare_id'],
        'mta_tax': df.at[index, 'mta_tax'],
        'improvement_surcharge': df.at[index, 'improvement_surcharge'],
        'total_amount': df.at[index, 'total_amount'],
        'tolls_amount': df.at[index, 'tolls_amount'],
        'fare_amount': df.at[index, 'fare_amount'],
        'extra': df.at[index, 'extra'],
        'payment_type': df.at[index, 'payment_type']
    }

    fare = fare.append(new_rol, ignore_index=True)
    
    return fare
