import os
import sys
import boto3

s3 = boto3.client('s3')

def get_file(bucket_name, file_key):
    return s3.get_object(Bucket=bucket_name, Key=file_key)

def upload_file(bucket_name, file_path, file_key):
    return s3.upload_file(file_path, bucket_name, file_key)

def list_objects(bucket_name, prefix='raw/'):
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix)

    keys = []
    arquivos = response['Contents']
    for arquivo in arquivos:
        if(arquivo['Key'] != prefix and arquivo['Key'] != prefix):
            keys.append(arquivo['Key'])
    
    return keys
