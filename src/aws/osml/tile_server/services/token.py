#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import json
import logging
import os.path
from base64 import b64decode, b64encode
from os import path
from time import sleep
from typing import Optional

from cryptography.fernet import Fernet

from aws.osml.tile_server.app_config import ServerConfig

TS_TOKEN_FILE_NAME = "ts-token"

logger = logging.getLogger("uvicorn")


def initialize_token_key() -> None:
    """
    Initializes and stores a new encryption token key in a JSON file if one does not already exist.

    :return: None
    """
    try:
        if not os.path.exists(f"/{ServerConfig.efs_mount_name}"):
            os.makedirs(f"/{ServerConfig.efs_mount_name}")
        file_path = path.join(f"/{ServerConfig.efs_mount_name}", TS_TOKEN_FILE_NAME)
        if not path.isfile(file_path):
            key = Fernet.generate_key()
            with open(file_path, "w") as token_file:
                json.dump({"token": b64encode(key).decode("ascii")}, token_file)
    except Exception as err:
        logger.error(f"Unable to initialize token key! {err}")
        return None


def read_token_key() -> Optional[bytes]:
    """
    Reads and returns the encryption token key from a JSON file.

    :return: The encryption token key as a dictionary if the file exists and contains valid JSON, None otherwise.
    """
    file_path = path.join(f"/{ServerConfig.efs_mount_name}", TS_TOKEN_FILE_NAME)
    try:
        with open(file_path, "r") as token_file:
            token: str = json.load(token_file).get("token")
            return b64decode(token)
    except Exception as err:
        # Handle the case where the file is empty or not valid JSON
        logger.error(f"Not able to read token key from json file {file_path} wth error: {err}")
        return None


def get_encryptor() -> Fernet:
    """
    This initializes a token and returns the associated Fernet object.
    If a token is already present a new one is not created.

    :return:
    """
    initialize_token_key()
    sleep(0.1)
    return Fernet(read_token_key())
