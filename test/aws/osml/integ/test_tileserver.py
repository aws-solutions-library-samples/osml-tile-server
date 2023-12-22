import json
import logging
import sys
import time
import traceback
from typing import Any, Dict

import urllib3
from botocore.exceptions import ClientError

from .tests import (
    test_bounds_viewpoint,
    test_create_viewpoint,
    test_crop_viewpoint,
    test_delete_viewpoint,
    test_describe_viewpoint,
    test_info_viewpoint,
    test_list_viewpoint,
    test_metadata_viewpoint,
    test_preview_viewpoint,
    test_statistics_viewpoint,
    test_tile_viewpoint,
    test_update_viewpoint,
)
from .utils.aws_services import fetch_elb_endpoint
from .utils.configurations import TestData


class TestTileServer:
    def __init__(self) -> None:
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

        try:
            self.elb_endpoint = fetch_elb_endpoint()
            self.url = self.elb_endpoint + "/latest/viewpoints"
        except ClientError as err:
            self.logger.error(f"Fatal error occurred while fetching elb endpoint. Exception: {err}")
            sys.exit("Fatal error occurred while fetching elb endpoint. Exiting.")

        try:
            self.http = urllib3.PoolManager()
        except ClientError as err:
            self.logger.error(f"Fatal error occurred while setting up the HTTP Pool Manager. Exception: {err}")
            sys.exit("Fatal error occurred while setting up the HTTP Pool Manager. Exiting.")

        self.image_type = None

    def ping_check(self) -> None:
        """
        Test Case: Succesfully ping check the endpoint url

        return bool: Test succeeded or failed
        """
        try:
            response = self.http.request("GET", self.elb_endpoint)

            assert response.status == 200
            assert "OSML Tile Server" in response.data.decode("utf-8")

            return True
        except Exception as err:
            self.logger.error(traceback.print_exception(err))
            return False

    def start_integration_test(self, image_type: str) -> Dict[str, Any]:
        """
        Run a various of integration tests including both HAPPY and UNHAPPY test cases

        :param image_type: Type of Image to run against the Tile Server

        return Dict[str, Any]: Return the status of the integration tests
        """
        # Check to see if the server is live
        test_ping_check = self.ping_check()
        self.image_type = image_type

        # Create a viewpoint
        test_create_happy, test_viewpoint_id = test_create_viewpoint.create_viewpoint(self, TestData.TEST_BODY_SMALL)

        # List newly created viewpoints
        test_list_happy = test_list_viewpoint.list_viewpoint(self)

        # Describe this Viewpoint
        test_describe_happy = test_describe_viewpoint.describe_viewpoint(self, test_viewpoint_id)

        # Validate that the Viewpoint Request is in READY state, if not, check again in 3 seconds
        while True:
            response = self.http.request("GET", f"{self.url}/{test_viewpoint_id}")
            response_data = json.loads(response.data)

            if response.status == 200:
                if response_data["viewpoint_status"] == "READY":
                    break
                else:
                    self.logger.warning(f"{test_viewpoint_id} status is not READY! Checking again in 3 seconds...")
                    time.sleep(3)  # sleep for 5 seconds then check again

        # Get Bounds of this Viewpoint
        test_bounds_happy = test_bounds_viewpoint.get_bounds_viewpoint(self, test_viewpoint_id)

        # Get Metadata of this Viewpoint
        test_metadata_happy = test_metadata_viewpoint.get_metadata_viewpoint(self, test_viewpoint_id)

        # Get Info of this Viewpoint (INFO not implemented yet)
        test_info_happy = test_info_viewpoint.get_info_viewpoint(self, test_viewpoint_id)

        # Get Statistics of this Viewpoint
        test_statistics_happy = test_statistics_viewpoint.get_statistics_viewpoint(self, test_viewpoint_id)

        # Perform Crop API call
        test_crop_happy = test_crop_viewpoint.get_crop_viewpoint(self, test_viewpoint_id)

        # Perform Tile API call
        test_tile_happy = test_tile_viewpoint.get_tile_viewpoint(self, test_viewpoint_id)

        # Perform Preview API Call
        test_preivew_happy = test_preview_viewpoint.get_preview_viewpoint(self, test_viewpoint_id)

        # Update the Viewpoint data then describe the Viewpoint data
        test_update_happy = test_update_viewpoint.update_viewpoint(self, test_viewpoint_id, TestData.VALID_UPDATE_TEST_BODY)

        # Update the Viewpoint data with invalid update body
        test_update_unhappy_2 = test_update_viewpoint.update_viewpoint_unhappy_2(
            self, test_viewpoint_id, TestData.INVALID_UPDATE_TEST_BODY
        )

        # Delete this Viewpoint
        test_delete_happy = test_delete_viewpoint.delete_viewpoint(self, test_viewpoint_id)

        # Fetch Bounds / Metadata / Info / Statistics of a deleted viewpoint
        test_bounds_unhappy = test_bounds_viewpoint.get_bounds_viewpoint_unhappy(self, test_viewpoint_id)
        test_metadata_unhappy = test_metadata_viewpoint.get_metadata_viewpoint_unhappy(self, test_viewpoint_id)
        test_info_unhappy = test_info_viewpoint.get_info_viewpoint_unhappy(self, test_viewpoint_id)
        test_statistics_unhappy = test_statistics_viewpoint.get_statistics_viewpoint_unhappy(self, test_viewpoint_id)

        # Fetch Tile / Crop / Preview of a deleted viewpoint
        test_crop_unhappy = test_crop_viewpoint.get_crop_viewpoint_unhappy(self, test_viewpoint_id)
        test_tile_unhappy = test_tile_viewpoint.get_tile_viewpoint_unhappy(self, test_viewpoint_id)
        test_preview_unhappy = test_preview_viewpoint.get_preview_viewpoint_unhappy(self, test_viewpoint_id)

        # Fetch cannot update Viewpoint since it's deleted
        test_update_unhappy = test_update_viewpoint.update_viewpoint_unhappy(
            self, test_viewpoint_id, TestData.VALID_UPDATE_TEST_BODY
        )

        # Call Describe Viewpoint on invalid viewpoint
        test_describe_unhappy = test_describe_viewpoint.describe_viewpoint_unhappy(self, TestData.TEST_INVALID_VIEWPOINT_ID)

        # Create new viewpoint with bad inputs
        test_create_unhappy = test_create_viewpoint.create_viewpoint_unhappy(self, TestData.INVALID_TEST_BODY)

        # Delete the DELETED viewpoint
        test_delete_unhappy = test_delete_viewpoint.delete_viewpoint_unhappy(self, test_viewpoint_id)

        # Collect all Testing Results
        test_results = {
            "test_ping_check": test_ping_check,
            "test_create_happy": test_create_happy,
            "test_list_happy": test_list_happy,
            "test_describe_happy": test_describe_happy,
            "test_bounds_happy": test_bounds_happy,
            "test_metadata_happy": test_metadata_happy,
            "test_info_happy": test_info_happy,
            "test_statistics_happy": test_statistics_happy,
            "test_crop_happy": test_crop_happy,
            "test_tile_happy": test_tile_happy,
            "test_preivew_happy": test_preivew_happy,
            "test_update_happy": test_update_happy,
            "test_update_unhappy_2": test_update_unhappy_2,
            "test_delete_happy": test_delete_happy,
            "test_bounds_unhappy": test_bounds_unhappy,
            "test_metadata_unhappy": test_metadata_unhappy,
            "test_info_unhappy": test_info_unhappy,
            "test_statistics_unhappy": test_statistics_unhappy,
            "test_crop_unhappy": test_crop_unhappy,
            "test_tile_unhappy": test_tile_unhappy,
            "test_preview_unhappy": test_preview_unhappy,
            "test_update_unhappy": test_update_unhappy,
            "test_describe_unhappy": test_describe_unhappy,
            "test_create_unhappy": test_create_unhappy,
            "test_delete_unhappy": test_delete_unhappy,
        }

        self.image_type = None

        # Check to see if any of the test failed, if so, return the status code of 500 (Server error)
        failed = False
        for k, v in test_results.items():
            if not v:
                failed = True
                break

        if failed:
            return {
                "statusCode": 500,
                "message": "One or more test failed. Please check the logs for further root cause.",
                "testResults": test_results,
            }
        else:
            return {"statusCode": 200, "message": "All test succeeded!", "testResults": test_results}


def test_tileserver(event: Dict[Any, Any], context: Any):
    """
    This Lambda is responsible for running integration tests

    :param event: The event object that triggered the Lambda function. It contains information about the event source
        and any data associated with the event.
    :param context: The runtime information of the Lambda function. It provides methods and properties that allow you to
        interact with the runtime environment.

    :return: A dictionary containing the response status code and body.
    """
    ts = TestTileServer()
    return ts.start_integration_test("small")
