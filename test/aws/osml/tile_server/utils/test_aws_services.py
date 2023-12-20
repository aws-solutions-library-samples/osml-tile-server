#  Copyright 2023 Amazon.com, Inc. or its affiliates.

from unittest import TestCase

import pytest
from moto import mock_dynamodb, mock_s3, mock_sqs


class TestRefreshableBotoSession(TestCase):
    @pytest.mark.skip(reason="Test pending")
    def test_refresh(self):
        pass

    @pytest.mark.skip(reason="Test pending")
    def test_refreshable_session(self):
        pass


@mock_s3
@mock_dynamodb
@mock_sqs
class TestAwsServices(TestCase):
    @pytest.mark.skip(reason="Test pending")
    def test_initialize_ddb(self):
        pass

    @pytest.mark.skip(reason="Test pending")
    def initialize_s3(self):
        pass

    @pytest.mark.skip(reason="Test pending")
    def initialize_sqs(self):
        pass

    @pytest.mark.skip(reason="Test pending")
    def initialize_aws_services(self):
        pass
