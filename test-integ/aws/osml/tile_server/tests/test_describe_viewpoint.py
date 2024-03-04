#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import json
import traceback


def describe_viewpoint(self, viewpoint_id: str) -> bool:
    """
    Test Case: Succesfully describe a viewpoint

    :param viewpoint_id: Unique viewpoint id to get from the table.

    return bool: Test succeeded or failed
    """
    try:
        response = self.http.request("GET", f"{self.url}/{viewpoint_id}")

        response_data = json.loads(response.data)

        assert response.status == 200
        assert response_data["viewpoint_id"] == viewpoint_id
        assert response_data["viewpoint_status"] != "DELETED"

        with open(f"aws/osml/tile_server/data/{self.image_type}/test_{self.image_type}_viewpoint.json", "r") as output_json:
            expected_json_result = json.loads(output_json.read())

            expected_json_result["viewpoint_id"] = response_data["viewpoint_id"]
            expected_json_result["bucket_name"] = response_data["bucket_name"]
            expected_json_result["viewpoint_status"] = response_data["viewpoint_status"]

            assert response_data == expected_json_result
        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False


def describe_viewpoint_unhappy(self, viewpoint_id: str) -> bool:
    """
    Test Case: Failed to describe a viewpoint

    :param viewpoint_id: Unique viewpoint id to get from the table.

    return bool: Test succeeded or failed
    """
    try:
        response = self.http.request("GET", f"{self.url}/{viewpoint_id}")

        response_data = json.loads(response.data)

        assert response.status == 500
        assert "Invalid Key, it does not exist in ViewpointStatusTable" in response_data["detail"]
        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False
