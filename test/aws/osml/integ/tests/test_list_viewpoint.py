import json
import traceback 

def list_viewpoint(self) -> bool:
    """
    Test Case: Succesfully get the list of the viewpoints

    return bool: Test succeeded or failed
    """
    try:
        response = self.http.request('GET', self.elb_endpoint + "/latest/viewpoints/")
        
        response_data = json.loads(response.data) 
        
        assert response.status == 200
        assert len(response_data["items"]) > 0
        return True
    except Exception as err:
        self.logger.error(traceback.print_exception(err))
        return False
