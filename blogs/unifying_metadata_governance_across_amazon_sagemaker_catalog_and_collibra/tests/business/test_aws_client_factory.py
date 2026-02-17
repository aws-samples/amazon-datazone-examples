"""
Unit tests for lambda/business/AWSClientFactory.py
"""
import pytest
from unittest.mock import patch, MagicMock

from business.AWSClientFactory import AWSClientFactory


@pytest.mark.unit
class TestAWSClientFactory:
    """Tests for AWSClientFactory class"""

    @patch('business.AWSClientFactory.boto3.client')
    def test_create_returns_boto3_client(self, mock_boto3_client):
        """Test create returns boto3 client for service"""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        result = AWSClientFactory.create('s3')
        
        assert result == mock_client
        mock_boto3_client.assert_called_once()

    @patch('business.AWSClientFactory.boto3.client')
    def test_create_uses_smus_region(self, mock_boto3_client):
        """Test create uses SMUS_REGION from environment"""
        AWSClientFactory.create('dynamodb')
        
        call_args = mock_boto3_client.call_args
        config = call_args[1]['config']
        assert config.region_name == 'us-east-1'

    @patch('business.AWSClientFactory.boto3.client')
    def test_create_with_different_services(self, mock_boto3_client):
        """Test create works with different AWS services"""
        services = ['s3', 'dynamodb', 'secretsmanager', 'datazone']
        
        for service in services:
            AWSClientFactory.create(service)
            assert mock_boto3_client.call_args[0][0] == service
