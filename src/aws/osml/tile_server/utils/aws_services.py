import boto3
from boto3.resources.base import ServiceResource
import os

class Config:
    AWS_REGION = os.getenv('us-west-2')

def initialize_ddb() -> ServiceResource:
    ddb = boto3.resource('dynamodb', region_name='us-west-2')

    return ddb

def initialize_s3() -> ServiceResource:
    s3 = boto3.resource('s3')
    
    return s3
