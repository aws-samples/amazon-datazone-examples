"""
Unit tests for lambda/model/AWSRedshiftClusterMetadataCollibraAttribute.py
"""
import pytest

from model.AWSRedshiftClusterMetadataCollibraAttribute import AWSRedshiftClusterMetadataCollibraAttribute


@pytest.mark.unit
class TestAWSRedshiftClusterMetadataCollibraAttribute:
    """Tests for AWSRedshiftClusterMetadataCollibraAttribute class"""

    def test_init_with_valid_endpoint(self):
        """Test initialization with valid Redshift endpoint"""
        metadata = {
            'redshiftEndpoint': 'my-cluster.abc123.us-east-1.redshift.amazonaws.com:5439/dev'
        }
        
        attr = AWSRedshiftClusterMetadataCollibraAttribute(metadata)
        
        assert attr.cluster_name == 'my-cluster'
        assert attr.region == 'us-east-1'

    def test_init_with_different_region(self):
        """Test initialization with different AWS region"""
        metadata = {
            'redshiftEndpoint': 'test-cluster.xyz789.us-west-2.redshift.amazonaws.com:5439/prod'
        }
        
        attr = AWSRedshiftClusterMetadataCollibraAttribute(metadata)
        
        assert attr.cluster_name == 'test-cluster'
        assert attr.region == 'us-west-2'

    def test_init_with_hyphenated_cluster_name(self):
        """Test initialization with hyphenated cluster name"""
        metadata = {
            'redshiftEndpoint': 'my-test-cluster.abc123.eu-west-1.redshift.amazonaws.com:5439/db'
        }
        
        attr = AWSRedshiftClusterMetadataCollibraAttribute(metadata)
        
        assert attr.cluster_name == 'my-test-cluster'
        assert attr.region == 'eu-west-1'

    def test_init_raises_error_with_invalid_endpoint(self):
        """Test initialization raises ValueError with invalid endpoint"""
        metadata = {
            'redshiftEndpoint': 'invalid-endpoint'
        }
        
        with pytest.raises(ValueError) as exc_info:
            AWSRedshiftClusterMetadataCollibraAttribute(metadata)
        
        assert "Invalid Redshift Endpoint" in str(exc_info.value)

    def test_init_raises_error_with_malformed_endpoint(self):
        """Test initialization raises ValueError with malformed endpoint"""
        metadata = {
            'redshiftEndpoint': 'cluster.amazonaws.com'
        }
        
        with pytest.raises(ValueError) as exc_info:
            AWSRedshiftClusterMetadataCollibraAttribute(metadata)
        
        assert "Invalid Redshift Endpoint" in str(exc_info.value)

    def test_properties_are_read_only(self):
        """Test that properties cannot be set directly"""
        metadata = {
            'redshiftEndpoint': 'my-cluster.abc123.us-east-1.redshift.amazonaws.com:5439/dev'
        }
        attr = AWSRedshiftClusterMetadataCollibraAttribute(metadata)
        
        with pytest.raises(AttributeError):
            attr.cluster_name = "new-cluster"
        
        with pytest.raises(AttributeError):
            attr.region = "us-west-2"

    def test_init_with_endpoint_without_port_and_database(self):
        """Test initialization with endpoint without port and database"""
        metadata = {
            'redshiftEndpoint': 'my-cluster.abc123.us-east-1.redshift.amazonaws.com'
        }
        
        attr = AWSRedshiftClusterMetadataCollibraAttribute(metadata)
        
        assert attr.cluster_name == 'my-cluster'
        assert attr.region == 'us-east-1'

    def test_init_with_single_character_cluster_name(self):
        """Test initialization with single character cluster name"""
        metadata = {
            'redshiftEndpoint': 'a.abc123.us-east-1.redshift.amazonaws.com:5439/dev'
        }
        
        attr = AWSRedshiftClusterMetadataCollibraAttribute(metadata)
        
        assert attr.cluster_name == 'a'
        assert attr.region == 'us-east-1'

    def test_init_with_max_length_cluster_name(self):
        """Test initialization with maximum length cluster name (63 chars)"""
        long_name = 'a' * 63
        metadata = {
            'redshiftEndpoint': f'{long_name}.abc123.us-east-1.redshift.amazonaws.com:5439/dev'
        }
        
        attr = AWSRedshiftClusterMetadataCollibraAttribute(metadata)
        
        assert attr.cluster_name == long_name
        assert attr.region == 'us-east-1'
