import os
import sys
import boto3

s3 = boto3.resource('s3')

def get_file(bucket_name, fileKey):
    return s3.Object(bucket_name, fileKey)