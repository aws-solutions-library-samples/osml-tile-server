#  Copyright 2023-2024 Amazon.com, Inc or its affiliates.

from unittest import TestCase

from aws.osml.tile_server.utils import HealthCheck


class TestHealthCheck(TestCase):
    def test_health_check(self):
        health_check = HealthCheck(status="OK")
        assert health_check.status == "OK"
