"""
Unit tests for lambda/model/AWSRedshiftServerlessMetadataCollibraAttribute.py
"""
import pytest

from model.AWSRedshiftServerlessMetadataCollibraAttribute import AWSRedshiftServerlessMetadataCollibraAttribute


@pytest.mark.unit
class TestAWSRedshiftServerlessMetadataCollibraAttribute:
    """Tests for AWSRedshiftServerlessMetadataCollibraAttribute class"""

    def test_init_with_valid_endpoint(self):
        """Test initialization with valid Redshift Serverless endpoint"""
        metadata = {
            'redshiftEndpoint': 'my-workgroup.123456789012.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
        }
        
        attr = AWSRedshiftServerlessMetadataCollibraAttribute(metadata)
        
        assert attr.workgroup_name == 'my-workgroup'
        assert attr.account_id == '123456789012'
        assert attr.region == 'us-east-1'

    def test_init_with_different_region(self):
        """Test initialization with different AWS region"""
        metadata = {
            'redshiftEndpoint': 'test-wg.999888777666.us-west-2.redshift-serverless.amazonaws.com:5439/prod'
        }
        
        attr = AWSRedshiftServerlessMetadataCollibraAttribute(metadata)
        
        assert attr.workgroup_name == 'test-wg'
        assert attr.account_id == '999888777666'
        assert attr.region == 'us-west-2'

    def test_init_with_hyphenated_workgroup_name(self):
        """Test initialization with hyphenated workgroup name"""
        metadata = {
            'redshiftEndpoint': 'my-test-wg.123456789012.eu-west-1.redshift-serverless.amazonaws.com:5439/db'
        }
        
        attr = AWSRedshiftServerlessMetadataCollibraAttribute(metadata)
        
        assert attr.workgroup_name == 'my-test-wg'
        assert attr.account_id == '123456789012'
        assert attr.region == 'eu-west-1'

    def test_init_with_numeric_workgroup_name(self):
        """Test initialization with numeric characters in workgroup name"""
        metadata = {
            'redshiftEndpoint': 'wg-123.123456789012.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
        }
        
        attr = AWSRedshiftServerlessMetadataCollibraAttribute(metadata)
        
        assert attr.workgroup_name == 'wg-123'
        assert attr.account_id == '123456789012'

    def test_init_raises_error_with_invalid_endpoint(self):
        """Test initialization raises ValueError with invalid endpoint"""
        metadata = {
            'redshiftEndpoint': 'invalid-endpoint'
        }
        
        with pytest.raises(ValueError) as exc_info:
            AWSRedshiftServerlessMetadataCollibraAttribute(metadata)
        
        assert "Invalid Redshift Endpoint" in str(exc_info.value)

    def test_init_raises_error_with_malformed_endpoint(self):
        """Test initialization raises ValueError with malformed endpoint"""
        metadata = {
            'redshiftEndpoint': 'workgroup.amazonaws.com'
        }
        
        with pytest.raises(ValueError) as exc_info:
            AWSRedshiftServerlessMetadataCollibraAttribute(metadata)
        
        assert "Invalid Redshift Endpoint" in str(exc_info.value)

    def test_properties_are_read_only(self):
        """Test that properties cannot be set directly"""
        metadata = {
            'redshiftEndpoint': 'my-wg.123456789012.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
        }
        attr = AWSRedshiftServerlessMetadataCollibraAttribute(metadata)
        
        with pytest.raises(AttributeError):
            attr.workgroup_name = "new-wg"
        
        with pytest.raises(AttributeError):
            attr.account_id = "999999999999"
        
        with pytest.raises(AttributeError):
            attr.region = "us-west-2"

    def test_init_with_endpoint_without_port_and_database(self):
        """Test initialization with endpoint without port and database"""
        metadata = {
            'redshiftEndpoint': 'my-wg.123456789012.us-east-1.redshift-serverless.amazonaws.com'
        }
        
        attr = AWSRedshiftServerlessMetadataCollibraAttribute(metadata)
        
        assert attr.workgroup_name == 'my-wg'
        assert attr.account_id == '123456789012'
        assert attr.region == 'us-east-1'

    def test_init_with_min_length_workgroup_name(self):
        """Test initialization with minimum length workgroup name (3 chars)"""
        metadata = {
            'redshiftEndpoint': 'abc.123456789012.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
        }
        
        attr = AWSRedshiftServerlessMetadataCollibraAttribute(metadata)
        
        assert attr.workgroup_name == 'abc'
        assert attr.account_id == '123456789012'

    def test_init_with_max_length_workgroup_name(self):
        """Test initialization with maximum length workgroup name (63 chars)"""
        long_name = 'a' * 63
        metadata = {
            'redshiftEndpoint': f'{long_name}.123456789012.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
        }
        
        attr = AWSRedshiftServerlessMetadataCollibraAttribute(metadata)
        
        assert attr.workgroup_name == long_name
        assert attr.account_id == '123456789012'
