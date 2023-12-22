import json
import traceback


def get_bounds_viewpoint(self, viewpoint_id: str) -> bool:
    """
    Test Case: Succesfully get the bounds of the viewpoint

    :param viewpoint_id: Unique viewpoint id to get from the table.

    return bool: Test succeeded or failed
    """
    try:
        response = self.http.request("GET", f"{self.url}/{viewpoint_id}/bounds")

        response_data = json.loads(response.data)

        assert response.status == 200

        with open(f"aws/osml/tile_server/data/{self.image_type}/test_{self.image_type}_bounds.json", "r") as output_json:
            expected_json_result = json.loads(output_json.read())
            assert response_data == expected_json_result

        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False


def get_bounds_viewpoint_unhappy(self, viewpoint_id: str) -> bool:
    """
    Test Case: Failed to get the bounds of the viewpoint

    :param viewpoint_id: Unique viewpoint id to get from the table.

    return bool: Test succeeded or failed
    """
    try:
        response = self.http.request("GET", f"{self.url}/{viewpoint_id}/bounds")

        response_data = json.loads(response.data)

        assert response.status == 404
        assert (
            "Cannot view ViewpointApiNames.BOUNDS for this image since this has already been deleted."
            in response_data["detail"]
        )
        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False
