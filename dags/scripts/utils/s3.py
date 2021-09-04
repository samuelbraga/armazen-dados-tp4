import os
import sys
import boto3

s3 = boto3.client('s3')

def get_file(bucket_name, file_key):
    return s3.get_object(bucket_name, key=file_key)