#  Copyright 2023 Amazon.com, Inc. or its affiliates.

import logging
import traceback
from datetime import datetime, timezone
from time import time
from typing import Dict, Tuple

from boto3 import Session
from boto3.resources.base import ServiceResource
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session

from aws.osml.tile_server.app_config import BotoConfig, ServerConfig


class RefreshableBotoSession:
    """
    RefreshableBotoSession is a Boto3 helper class which allows us to create a refreshable session such that none of the
    client/resources we want to call against will never expire.
    """

    def __init__(self):
        self.sts_arn = ServerConfig.sts_arn
        self.session_name = "TileServerSession"
        self.logger = logging.getLogger("uvicorn")
        self.session_duration = 3600  # 60 minutes

    def _refresh(self) -> Dict[str, str]:
        """
        Get session credentials

        :return: session credentials
        """
        session = Session(region_name=ServerConfig.aws_region)
        credentials = {}

        if self.sts_arn:
            sts_client = session.client(service_name="sts", region_name=ServerConfig.aws_region)
            response = sts_client.assume_role(
                RoleArn=self.sts_arn,
                RoleSessionName=self.session_name,
                DurationSeconds=self.session_duration,
            ).get("Credentials")

            credentials = {
                "access_key": response.get("AccessKeyId"),
                "secret_key": response.get("SecretAccessKey"),
                "token": response.get("SessionToken"),
                "expiry_time": response.get("Expiration").isoformat(),
            }
        else:
            try:
                session_credentials = session.get_credentials().__dict__
                credentials = {
                    "access_key": session_credentials.get("access_key"),
                    "secret_key": session_credentials.get("secret_key"),
                    "token": session_credentials.get("token"),
                    "expiry_time": datetime.fromtimestamp(time() + 3600, timezone.utc).isoformat(),
                }
            except Exception as err:
                credentials = {
                    "access_key": None,
                    "secret_key": None,
                    "token": None,
                    "expiry_time": datetime.fromtimestamp(time() + 3600, timezone.utc).isoformat(),
                }
                self.logger.error(f"Error has occurred when setting up the STS session - {err} / {traceback.format_exc()}")

        return credentials

    def refreshable_session(self) -> Session:
        """
        Create a refreshable session which will automatically renew credentials when the credential expired.

        :return: Refreshable Session
        """
        session_credentials = RefreshableCredentials.create_from_metadata(
            metadata=self._refresh(),
            refresh_using=self._refresh,
            method="sts-assume-role",
        )

        session = get_session()

        session._credentials = session_credentials
        session.set_config_variable("region", ServerConfig.aws_region)

        autorefresh_session = Session(botocore_session=session)

        return autorefresh_session


def initialize_ddb(session: Session) -> ServiceResource:
    """
    Initialize DynamoDB service and return a service resource.

    :return: DynamoDB service resource
    """
    ddb = session.resource("dynamodb", config=BotoConfig.default, region_name=ServerConfig.aws_region)

    return ddb


def initialize_s3(session: Session) -> ServiceResource:
    """
    Initialize S3 service and return a service resource.

    :return: S3 service resource
    """
    s3 = session.resource("s3", config=BotoConfig.default)

    return s3


def initialize_sqs(session: Session) -> ServiceResource:
    """
    Initialize SQS service and return a service resource.

    :return: SQS service resource
    """
    sqs = session.resource("sqs", config=BotoConfig.default)

    return sqs


def initialize_aws_services() -> Tuple[ServiceResource, ServiceResource, ServiceResource]:
    """
    Initialize AWS services required by the application.

    This function initializes DynamoDB, S3, and SQS services,
    handling any exceptions that occur during the process.

    :return: Tuple containing the initialized service resources for ddb, s3, and sqs clients.

    :raises: SystemExit if any service fails to initialize.
    """

    session = RefreshableBotoSession().refreshable_session()

    ddb = initialize_ddb(session)
    s3 = initialize_s3(session)
    sqs = initialize_sqs(session)

    return ddb, s3, sqs
