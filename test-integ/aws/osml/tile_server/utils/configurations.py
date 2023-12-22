from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class TestConfig:
    # S3 bucket names
    test_bucket: str = "test-images-825536440648"
    test_object_key: str = "small.tif"

    # Viewpoint name
    test_viewpoint_name: str = "test-name"


class TestData:
    from .configurations import TestConfig

    TEST_INVALID_VIEWPOINT_ID: str = "invalid-viewpoint-id"

    TEST_BODY_SMALL: Dict[str, Any] = {
        "bucket_name": TestConfig.test_bucket,
        "object_key": TestConfig.test_object_key,
        "viewpoint_name": TestConfig.test_viewpoint_name,
        "tile_size": 512,
        "range_adjustment": "NONE",
    }

    INVALID_TEST_BODY: Dict[str, Any] = {
        "bucket_name": None,
        "object_key": TestConfig.test_object_key,
        "viewpoint_name": TestConfig.test_viewpoint_name,
        "tile_size": 512,
        "range_adjustment": "NONE",
    }

    VALID_UPDATE_TEST_BODY: Dict[str, Any] = {
        "viewpoint_id": "",
        "viewpoint_name": "New-Viewpoint-Name",
        "tile_size": 512,
        "range_adjustment": "NONE",
    }

    INVALID_UPDATE_TEST_BODY: Dict[str, Any] = {
        "tile_size": 512,
        "range_adjustment": "NONE",
    }
