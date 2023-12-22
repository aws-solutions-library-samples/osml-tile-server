import json
import traceback 

def get_info_viewpoint(self, viewpoint_id: str) -> bool:
    """
    Test Case: Succesfully get the info of the viewpoint

    :param viewpoint_id: Unique viewpoint id to get from the table.

    return bool: Test succeeded or failed
    """
    try:
        response = self.http.request("GET", f"{self.url}/{viewpoint_id}/info")
        
        response_data = json.loads(response.data)
        
        assert response.status == 200
        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False


def get_info_viewpoint_unhappy(self, viewpoint_id: str) -> bool:
    """
    Test Case: Failed to get the info of the viewpoint

    :param viewpoint_id: Unique viewpoint id to get from the table.

    return bool: Test succeeded or failed
    """
    try:
        response = self.http.request("GET", f"{self.url}/{viewpoint_id}/info")
        
        response_data = json.loads(response.data)
        
        assert response.status == 400
        assert "Cannot view ViewpointApiNames.INFO for this image since this has already been deleted." in response_data["detail"]
        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False

