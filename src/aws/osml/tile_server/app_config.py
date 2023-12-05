import os
from dataclasses import dataclass

from botocore.config import Config


@dataclass
class ServerConfig:
    """
    ServerConfig is a dataclass meant to house the high-level configuration settings required for OSML Tile Server to
    operate that are provided through ENV variables. Note that required env parameters are enforced by the implied
    schema validation as os.environ[] is used to fetch the values. Optional parameters are fetched using, os.getenv(),
    which returns None.

    The data schema is defined as follows:
    region:  (str) The AWS region where the Tile Server is deployed.
    viewpoint_table: (str) The name of the viewpoint status DDB table
    """

    # optional env configuration with defaults
    aws_region: str = os.getenv("AWS_DEFAULT_REGION", "us-west-2")
    viewpoint_status_table: str = os.getenv("JOB_TABLE", "TSJobTable")
    viewpoint_request_queue: str = os.getenv("JOB_QUEUE", "TSJobQueue")
    efs_mount_name: str = os.getenv("EFS_MOUNT_NAME", "tmp/viewpoint")


@dataclass
class BotoConfig:
    """
    BotoConfig is a dataclass meant to vend our application the set of boto client configurations required for OSML

    The data schema is defined as follows:
    default:  (Config) the standard boto client configuration
    """

    # required env configuration
    default: Config = Config(region_name=ServerConfig.aws_region, retries={"max_attempts": 15, "mode": "standard"})
