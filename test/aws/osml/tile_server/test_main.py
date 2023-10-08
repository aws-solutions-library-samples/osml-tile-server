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


if __name__ == "__main__":
    unittest.main()
