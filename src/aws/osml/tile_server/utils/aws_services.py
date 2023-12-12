import boto3
from boto3.resources.base import ServiceResource

from aws.osml.tile_server.app_config import BotoConfig, ServerConfig


def initialize_ddb() -> ServiceResource:
    """
    Initialize DynamoDB service and return a service resource.

    :return: DynamoDB service resource
    """
    ddb = boto3.resource("dynamodb", config=BotoConfig.default, region_name=ServerConfig.aws_region)

    return ddb


def initialize_s3() -> ServiceResource:
    """
    Initialize S3 service and return a service resource.

    :return: S3 service resource
    """
    s3 = boto3.resource("s3", config=BotoConfig.default)

    return s3


def initialize_sqs() -> ServiceResource:
    """
    Initialize SQS service and return a service resource.

    :return: SQS service resource
    """
    sqs = boto3.resource("sqs", config=BotoConfig.default)

    return sqs
