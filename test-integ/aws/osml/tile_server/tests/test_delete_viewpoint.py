import json
import traceback


def delete_viewpoint(self, viewpoint_id: str) -> bool:
    """
    Test Case: Succesfully delete the viewpoint

    :param viewpoint_id: Unique viewpoint id to get from the table.

    return bool: Test succeeded or failed
    """
    try:
        response = self.http.request("DELETE", f"{self.url}/{viewpoint_id}")

        response_data = json.loads(response.data)

        assert response.status == 200
        assert response_data["viewpoint_status"] == "DELETED"
        assert response_data["local_object_path"] is None
        assert response_data["expire_time"] is not None
        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False


def delete_viewpoint_unhappy(self, viewpoint_id: str) -> bool:
    """
    Test Case: Failed to delete the viewpoint

    :param viewpoint_id: Unique viewpoint id to get from the table.

    return bool: Test succeeded or failed
    """
    try:
        response = self.http.request("DELETE", f"{self.url}/{viewpoint_id}")

        response_data = json.loads(response.data)
        assert response.status == 400
        assert (
            "Cannot view ViewpointApiNames.UPDATE for this image since this has already been deleted"
            in response_data["detail"]
        )

        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False
