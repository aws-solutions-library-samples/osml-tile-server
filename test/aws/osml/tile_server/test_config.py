#  Copyright 2023 Amazon.com, Inc. or its affiliates.

from dataclasses import dataclass


@dataclass
class TestConfig:
    test_region = "us-west-2"

    test_bucket = "test-bucket"
    test_object_key = "test-sample.nitf"
    test_file_path = "./test/data/test-sample.nitf"
    test_viewpoint_name = "test-name"

    test_viewpoint_table_name = "TSJobTable"
    test_viewpoint_key_schema = [{"AttributeName": "viewpoint_id", "KeyType": "HASH"}]
    test_viewpoint_attribute_def = [{"AttributeName": "viewpoint_id", "AttributeType": "S"}]

    test_viewpoint_request_queue_name = "TSJobQueue"
