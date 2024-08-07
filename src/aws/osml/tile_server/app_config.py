#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import os
from dataclasses import dataclass

from botocore.config import Config


@dataclass
class ServerConfig:
    """
    ServerConfig class to house the high-level configuration settings.

    The ServerConfig is a dataclass meant to house the high-level configuration settings
    required for OSML Tile Server to operate that are provided through ENV variables. Note
    that required env parameters are enforced by the implied schema validation as os.environ[]
    is used to fetch the values. Optional parameters are fetched using os.getenv(),
    which returns None.

    :param aws_region: The AWS region, defaults to 'us-west-2'
    :param viewpoint_status_table: The name of the viewpoint status DDB table, defaults to 'TSJobTable'
    :param viewpoint_request_queue: The name of the viewpoint request queue, defaults to 'TSJobQueue'
    :param efs_mount_name: The name of the EFS mount, defaults to 'ts-efs-volume'
    """

    aws_region: str = os.getenv("AWS_DEFAULT_REGION", "us-west-2")
    viewpoint_status_table: str = os.getenv("JOB_TABLE", "TSJobTable")
    viewpoint_request_queue: str = os.getenv("JOB_QUEUE", "TSJobQueue")
    efs_mount_name: str = os.getenv("EFS_MOUNT_NAME", "ts-efs-volume")
    sts_arn: str = os.getenv("STS_ARN", None)
    ddb_ttl_days: int = os.getenv("DDB_TTL_DAYS", 1)
    tile_server_log_level = logging.INFO

    OVERVIEW_FILE_EXTENSION = ".ovr"
    AUXXML_FILE_EXTENSION = ".aux.xml"
    METADATA_FILE_EXTENSION = ".metadata"
    BOUNDS_FILE_EXTENSION = ".bounds"
    INFO_FILE_EXTENSION = ".geojson"
    STATISTICS_FILE_EXTENSION = ".stats"


@dataclass
class BotoConfig:
    """
    BotoConfig is a dataclass meant to vend our application the set of boto client configurations required for OSML

    The data schema is defined as follows:
    :param default:  Standard boto client configuration
    """

    # Required env configuration
    default: Config = Config(region_name=ServerConfig.aws_region, retries={"max_attempts": 15, "mode": "standard"})
