import logging
import traceback
from datetime import datetime, timezone
from time import time
from typing import Dict, Tuple

import boto3
from boto3.resources.base import ServiceResource


def fetch_elb_endpoint() -> str:
    """
    Fetch an active endpoint for Tile Server

    return str: ELB Endpoint of Tile SErver
    """
    endpoint_url = "internal-aulran-TSDat-apI8FMCat4VJ-274307644.us-west-2.elb.amazonaws.com"
    return endpoint_url

    # Get VPC of this Lambda Function
    # lambda_client = boto3.client('lambda')
    # lambda_response = lambda_client.get_function_configuration(
    #     FunctionName="TSInteg"
    # )

    # vpcId = lambda_response["VpcConfig"]

    # # Get Security Subnet of this Lambda Function

    # #

    # elbv2 = boto3.client("elbv2")
