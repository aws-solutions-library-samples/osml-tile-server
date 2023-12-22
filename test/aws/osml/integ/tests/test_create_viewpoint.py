import json
import traceback 

from typing import Dict, Any, Tuple

def create_viewpoint(self, testBodyData: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Test Case: Succesully create a viewpoint

    :param test_body_data: Test body data to pass through POST http method

    return bool: Test succeeded or failed
    """
    try:
        response = self.http.request("POST", f"{self.url}/",
            headers={'Content-Type': 'application/json'},
            body=json.dumps(testBodyData))
        
        response_data = json.loads(response.data)
        
        assert response.status == 201
        assert response_data["viewpoint_id"] is not None
        assert response_data["viewpoint_status"] == "REQUESTED"
        
        return True, response_data["viewpoint_id"]
    except Exception as err:
            self.logger.error(traceback.print_exception(err))
            return False



def create_viewpoint_unhappy(self, testBodyData: Dict[str, Any]) -> bool:
    """
    Test Case: Failed to create a viewpoint

    :param test_body_data: Test body data to pass through POST http method

    return bool: Test succeeded or failed
    """
    try:
        response = self.http.request("POST", f"{self.url}/",
            headers={'Content-Type': 'application/json'},
            body=json.dumps(testBodyData))
        
        response_data = json.loads(response.data)
        
        assert response.status == 422
        assert response_data['detail'][0]['msg'] == "Input should be a valid string"
        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False
    
