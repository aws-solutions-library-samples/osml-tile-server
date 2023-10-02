
import unittest
from fastapi.testclient import TestClient


class TestTileServer(unittest.TestCase):
    def setUp(self):
        from aws.osml.tile_server import app
        self.client = TestClient(app)

    def tearDown(self):
        self.client = None
    
    def test_main(self):
        response = self.client.get("/")
        assert response.status_code == 200
        assert response.json() == {"message": "Hello Tile Server!"}
    
    def test_create_valid_viewpoint(self):
        response = self.client.post(
            "/viewpoints/",
            headers={"accept": "application/json", "Content-Type": "application/json"},
            data={
                "bucket_name": "test-images-825536440648",
                "object_key": "meta.ntf",
                "viewpoint_name": "test2",
                "tile_size": 512,
                "range_adjustment": "NONE"
            }
        )
        assert response.status_code == 200
        assert response.json() == {"message": "Hello Tile Server!"}

if __name__ == "__main__":
    unittest.main()