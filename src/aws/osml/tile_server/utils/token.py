#  Copyright 2023 Amazon.com, Inc. or its affiliates.

import pickle
from os import path

from cryptography.fernet import Fernet

from ..app_config import ServerConfig

TS_TOKEN_FILE_NAME = "ts-token"


def initialize_token_key() -> None:
    file_path = path.join(f"/{ServerConfig.efs_mount_name}", TS_TOKEN_FILE_NAME)
    if not path.isfile(file_path):
        pickle.dump(Fernet.generate_key(), open(file_path, "wb"))


def read_token_key() -> bytes:
    file_path = path.join(f"/{ServerConfig.efs_mount_name}", TS_TOKEN_FILE_NAME)
    return pickle.load(open(file_path, "rb"))
