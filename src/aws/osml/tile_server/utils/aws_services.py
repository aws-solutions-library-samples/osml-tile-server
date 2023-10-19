import boto3
from boto3.resources.base import ServiceResource

from aws.osml.tile_server.app_config import BotoConfig, ServerConfig


def initialize_ddb() -> ServiceResource:
    ddb = boto3.resource("dynamodb", config=BotoConfig.default, region_name=ServerConfig.aws_region)

    return ddb


def initialize_s3() -> ServiceResource:
    s3 = boto3.resource("s3", config=BotoConfig.default)

    return s3
