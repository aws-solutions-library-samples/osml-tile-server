#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.
import os
from dataclasses import dataclass


@dataclass
class TestConfig:
    __test__ = False

    test_region = "us-west-2"

    test_bucket = "test-bucket"
    test_object_key = "test-sample.nitf"
    test_data_directory = "./test/data"
    test_file_path = os.path.join(test_data_directory, "test-sample.nitf")
    test_metadata_path = os.path.join(test_data_directory, "test-sample.nitf.metadata")
    test_stats_path = os.path.join(test_data_directory, "test-sample.nitf.stats")
    test_bounds_path = os.path.join(test_data_directory, "test-sample.nitf.bounds")
    test_info_path = os.path.join(test_data_directory, "test-sample.nitf.geojson")
    test_viewpoint_name = "test-name"

    test_viewpoint_table_name = "TSJobTable"
    test_viewpoint_key_schema = [{"AttributeName": "viewpoint_id", "KeyType": "HASH"}]
    test_viewpoint_attribute_def = [{"AttributeName": "viewpoint_id", "AttributeType": "S"}]

    test_viewpoint_request_queue_name = "TSJobQueue"
