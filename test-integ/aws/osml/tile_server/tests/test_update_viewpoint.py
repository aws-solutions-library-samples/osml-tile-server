import json
import traceback
from typing import Any, Dict


def update_viewpoint(self, viewpoint_id: str, test_body_data: Dict[str, Any]) -> bool:
    """
    Test Case: Succesfully update the viewpoint

    :param viewpoint_id: Unique viewpoint id to get from the table.
    :param test_body_data: Test body data to pass through POST http method

    return bool: Test succeeded or failed
    """
    try:
        update_viewpoint_id = test_body_data
        update_viewpoint_id["viewpoint_id"] = viewpoint_id

        response = self.http.request(
            "PUT", f"{self.url}/", headers={"Content-Type": "application/json"}, body=json.dumps(test_body_data)
        )

        response_data = json.loads(response.data)

        assert response.status == 201
        assert response_data["viewpoint_name"] == "New-Viewpoint-Name"
        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False


def update_viewpoint_unhappy(self, viewpoint_id: str, test_body_data: Dict[str, Any]) -> bool:
    """
    Test Case: Failed to update the viewpoint

    :param viewpoint_id: Unique viewpoint id to get from the table.
    :param test_body_data: Test body data to pass through POST http method

    return bool: Test succeeded or failed
    """
    try:
        update_viewpoint_id = test_body_data
        update_viewpoint_id["viewpoint_id"] = viewpoint_id

        response = self.http.request(
            "PUT", f"{self.url}/", headers={"Content-Type": "application/json"}, body=json.dumps(test_body_data)
        )

        response_data = json.loads(response.data)

        assert response.status == 404
        assert (
            "Cannot view ViewpointApiNames.UPDATE for this image since this has already been deleted."
            in response_data["detail"]
        )
        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False


def update_viewpoint_unhappy_2(self, viewpoint_id: str, test_body_data: Dict[str, Any]) -> bool:
    """
    Test Case: Failed to update the viewpoint

    :param viewpoint_id: Unique viewpoint id to get from the table.
    :param test_body_data: Test body data to pass through POST http method

    return bool: Test succeeded or failed
    """
    try:
        update_viewpoint_id = test_body_data
        update_viewpoint_id["viewpoint_id"] = viewpoint_id

        response = self.http.request(
            "PUT", f"{self.url}/", headers={"Content-Type": "application/json"}, body=json.dumps(test_body_data)
        )

        response_data = json.loads(response.data)

        assert response.status == 422
        assert response_data["detail"][0]["msg"] == "Field required"
        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False
