import json
import traceback


def get_statistics_viewpoint(self, viewpoint_id: str) -> bool:
    """
    Test Case: Succesfully get the statistics of the viewpoint

    :param viewpoint_id: Unique viewpoint id to get from the table.

    return bool: Test succeeded or failed
    """
    try:
        response = self.http.request("GET", f"{self.url}/{viewpoint_id}/statistics")

        response_data = json.loads(response.data)  # convert single quote to double quotes

        assert response.status == 200

        with open(f"integ/data/{self.image_type}/test_{self.image_type}_statistics.json", "r") as output_json:
            expected_json_result = json.loads(output_json.read())

            assert (
                response_data["image_statistics"]["geoTransform"] == expected_json_result["image_statistics"]["geoTransform"]
            )
            assert (
                response_data["image_statistics"]["cornerCoordinates"]
                == expected_json_result["image_statistics"]["cornerCoordinates"]
            )
            assert response_data["image_statistics"]["bands"] == expected_json_result["image_statistics"]["bands"]
        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False


def get_statistics_viewpoint_unhappy(self, viewpoint_id: str) -> bool:
    """
    Test Case: Failed to get the statistics of the viewpoint

    :param viewpoint_id: Unique viewpoint id to get from the table.

    return bool: Test succeeded or failed
    """
    try:
        response = self.http.request("GET", f"{self.url}/{viewpoint_id}/statistics")

        response_data = json.loads(response.data)

        assert response.status == 400
        assert (
            "Cannot view ViewpointApiNames.STATISTICS for this image since this has already been deleted."
            in response_data["detail"]
        )
        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False
