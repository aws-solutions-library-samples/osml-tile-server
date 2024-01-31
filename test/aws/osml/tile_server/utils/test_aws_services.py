#  Copyright 2023-2024 Amazon.com, Inc or its affiliates.

from unittest import TestCase

import pytest
from moto import mock_aws


class TestRefreshableBotoSession(TestCase):
    @pytest.mark.skip(reason="Test not implemented")
    def test_refresh(self):
        pass

    @pytest.mark.skip(reason="Test not implemented")
    def test_refreshable_session(self):
        pass


@mock_aws
class TestAwsServices(TestCase):
    @pytest.mark.skip(reason="Test not implemented")
    def test_initialize_ddb(self):
        pass

    @pytest.mark.skip(reason="Test not implemented")
    def initialize_s3(self):
        pass

    @pytest.mark.skip(reason="Test not implemented")
    def initialize_sqs(self):
        pass

    @pytest.mark.skip(reason="Test not implemented")
    def initialize_aws_services(self):
        pass
