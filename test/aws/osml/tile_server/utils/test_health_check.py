#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.
import unittest
from unittest import TestCase

from aws.osml.tile_server.utils import HealthCheck


class TestHealthCheck(TestCase):
    """Unit tests for the HealthCheck utility."""

    def test_health_check_initialization(self):
        """Test that HealthCheck initializes with the correct status."""
        health_check = HealthCheck(status="OK")
        self.assertEqual(health_check.status, "OK")


if __name__ == "__main__":
    unittest.main()
