import json
import traceback


def get_tile_viewpoint(self, viewpoint_id: str) -> bool:
    """
    Test Case: Succesfully get the tile of the viewpoint

    :param viewpoint_id: Unique viewpoint id to get from the table.

    return bool: Test succeeded or failed
    """
    try:
        response = self.http.request("GET", f"{self.url}/{viewpoint_id}/tiles/10/10/10.PNG")

        assert response.status == 200
        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False


def get_tile_viewpoint_unhappy(self, viewpoint_id: str) -> bool:
    """
    Test Case: Failed to get the tile of the viewpoint

    :param viewpoint_id: Unique viewpoint id to get from the table.

    return bool: Test succeeded or failed
    """
    try:
        response = self.http.request("GET", f"{self.url}/{viewpoint_id}/tiles/10/10/10.PNG")

        response_data = json.loads(response.data)

        assert response.status == 500
        assert "Failed to fetch tile for image." in response_data["detail"]
        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False
