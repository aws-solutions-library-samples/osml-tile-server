#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import os
from dataclasses import dataclass


@dataclass
class TestConfig:
    __test__ = False

    test_region: str = "us-west-2"

    test_bucket: str = "test-bucket"
    test_object_key: str = "test-sample.nitf"
    test_data_directory: str = "./test/data"

    test_file_path: str = os.path.join(test_data_directory, "test-sample.nitf")
    test_metadata_path: str = os.path.join(test_data_directory, "test-sample.nitf.metadata")
    test_stats_path: str = os.path.join(test_data_directory, "test-sample.nitf.stats")
    test_bounds_path: str = os.path.join(test_data_directory, "test-sample.nitf.bounds")
    test_info_path: str = os.path.join(test_data_directory, "test-sample.nitf.geojson")

    test_viewpoint_id: str = "1234"
    test_viewpoint_name: str = "test-name"

    test_viewpoint_table_name: str = "TSJobTable"
    test_viewpoint_key_schema = [{"AttributeName": "viewpoint_id", "KeyType": "HASH"}]
    test_viewpoint_attribute_def = [{"AttributeName": "viewpoint_id", "AttributeType": "S"}]
    test_viewpoint_request_queue_name: str = "TSJobQueue"
