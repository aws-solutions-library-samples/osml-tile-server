import json
import traceback


def get_preview_viewpoint(self, viewpoint_id: str) -> bool:
    """
    Test Case: Succesfully get the preview of the viewpoint

    :param viewpoint_id: Unique viewpoint id to get from the table.

    return bool: Test succeeded or failed
    """
    try:
        response = self.http.request("GET", f"{self.url}/{viewpoint_id}/preview.JPEG")

        assert response.status == 200
        assert response.headers.get("content-type") == "image/jpeg"
        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False


def get_preview_viewpoint_unhappy(self, viewpoint_id: str) -> bool:
    """
    Test Case: Failed to get the preview of the viewpoint

    :param viewpoint_id: Unique viewpoint id to get from the table.

    return bool: Test succeeded or failed
    """
    try:
        response = self.http.request("GET", f"{self.url}/{viewpoint_id}/preview.JPEG")

        response_data = json.loads(response.data)

        assert response.status == 404
        assert (
            "Cannot view ViewpointApiNames.PREVIEW for this image since this has already been deleted."
            in response_data["detail"]
        )
        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False
