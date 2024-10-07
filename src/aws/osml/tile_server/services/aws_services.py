#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import sys
import traceback
from datetime import datetime, timezone
from time import time
from typing import Dict

from boto3 import Session
from boto3.resources.base import ServiceResource
from botocore.credentials import RefreshableCredentials
from botocore.exceptions import ClientError
from botocore.session import get_session

from aws.osml.tile_server.app_config import BotoConfig, ServerConfig

from .database import ViewpointStatusTable
from .queue import ViewpointRequestQueue

logger = logging.getLogger("uvicorn")


class RefreshableBotoSession:
    """
    RefreshableBotoSession is a Boto3 helper class that allows for the creation of a refreshable session such that none
    of the client/resources we want to call against will expire.
    """

    def __init__(self):
        self.sts_arn = ServerConfig.sts_arn
        self.session_name = "TileServerSession"
        self.session_duration = 3600  # 60 minutes

    def _refresh(self) -> Dict[str, str]:
        """
        Refresh the AWS credentials for the current session.

        :return: Session credentials that were refreshed.
        """
        session = Session(region_name=ServerConfig.aws_region)

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
                logger.error(f"Error has occurred when setting up STS session - {err} / {traceback.format_exc()}")

        return credentials

    def refreshable_session(self) -> Session:
        """
        Create a refreshable session which will automatically renew credentials when the credential expired.

        :return: Refreshable credentials session to use for the task user.
        """
        session_credentials = RefreshableCredentials.create_from_metadata(
            metadata=self._refresh(),
            refresh_using=self._refresh,
            method="sts-assume-role",
        )

        session = get_session()

        session._credentials = session_credentials
        session.set_config_variable("region", ServerConfig.aws_region)

        return Session(botocore_session=session)


class AwsServices:
    def __init__(self, ddb: ServiceResource = None, s3: ServiceResource = None, sqs: ServiceResource = None) -> None:
        """
        Initialize AWS services required by the application.

        This function initializes DynamoDB, S3, and SQS services,
        handling any exceptions that occur during the process.

        :param: ddb: An optional DynamoDB service resource to use.  If none is provided a new one will be initialized.
        :param: s3: An optional S3 service resource to use.  If none is provided a new one will be initialized.
        :param: sqs: An optional SQS service resource to use.  If none is provided a new one will be initialized.
        :return: None.

        :raises: SystemExit if any service fails to initialize.
        """
        try:
            session = RefreshableBotoSession().refreshable_session()
            self.ddb = self.initialize_ddb(session) if ddb is None else ddb
            self.s3 = self.initialize_s3(session) if s3 is None else s3
            self.sqs = self.initialize_sqs(session) if sqs is None else sqs
            if self.ddb is not None:
                self.viewpoint_database = ViewpointStatusTable(self.ddb, logger)
            if self.sqs is not None:
                self.viewpoint_queue = ViewpointRequestQueue(self.sqs, ServerConfig.viewpoint_request_queue, logger)
        except ClientError as err:
            sys.exit(f"Fatal error occurred while initializing AWS services. Exiting. {err}")

    @staticmethod
    def initialize_ddb(session: Session) -> ServiceResource:
        """
        Initialize DynamoDB service and return a service resource.

        :param: session: The credential session to use for the ServiceResource.
        :return: DynamoDB service resource for consumption.
        """

        return session.resource("dynamodb", config=BotoConfig.default, region_name=ServerConfig.aws_region)

    @staticmethod
    def initialize_s3(session: Session) -> ServiceResource:
        """
        Initialize S3 service and return a service resource.

        :param: session: The credential session to use for the ServiceResource.
        :return: S3 service resource for consumption.
        """

        return session.resource("s3", config=BotoConfig.default)

    @staticmethod
    def initialize_sqs(session: Session) -> ServiceResource:
        """
        Initialize SQS service and return a service resource.

        :param: session: The credential session to use for the ServiceResource.
        :return: SQS service resource for consumption.
        """

        return session.resource("sqs", config=BotoConfig.default)


def get_aws_services() -> AwsServices:
    return AwsServices()
