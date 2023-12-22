import logging
import sys

import boto3


def fetch_elb_endpoint(event, context) -> str:
    """
    Fetch an active endpoint for Tile Server

    :param event: The event object that triggered the Lambda function. It contains information about the event source
        and any data associated with the event.
    :param context: The runtime information of the Lambda function. It provides methods and properties that allow you to
        interact with the runtime environment.

    :return str: ELB Endpoint of Tile SErver
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:
        # Get function name
        function_name = context.function_name

        # Get VPC of this Lambda Function
        lambda_client = boto3.client("lambda")
        lambda_response = lambda_client.get_function_configuration(FunctionName=function_name)

        lambda_vpc = lambda_response["VpcConfig"]["VpcId"]

        client = boto3.client("elbv2")
        response = client.describe_load_balancers()
        for elb in response["LoadBalancers"]:
            elb_vpc = elb["VpcId"]
            loadbalancer_name = elb["LoadBalancerName"]
            if lambda_vpc == elb_vpc and "TSDat" in loadbalancer_name:
                return elb["DNSName"]

        return None
    except Exception as err:
        logger.error(f"Fatal error occurred while fetching elb endpoint. Exception: {err}")
        sys.exit("Fatal error occurred while fetching elb endpoint. Exiting.")
