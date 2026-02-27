"""
Unit tests for lambda/model/AWSGlueMetadataCollibraAttribute.py
"""
import pytest

from model.AWSGlueMetadataCollibraAttribute import AWSGlueMetadataCollibraAttribute


@pytest.mark.unit
class TestAWSGlueMetadataCollibraAttribute:
    """Tests for AWSGlueMetadataCollibraAttribute class"""

    def test_init_with_valid_arn_and_region(self):
        """Test initialization with valid ARN and region"""
        metadata = {
            'glueAccessRoleArn': 'arn:aws:iam::123456789012:role/role-name',
            'region': 'NORTHERNVIRGINIA'
        }
        
        attr = AWSGlueMetadataCollibraAttribute(metadata)
        
        assert attr.account_id == '123456789012'
        assert attr.region == 'us-east-1'

    def test_init_with_aws_cn_partition(self):
        """Test initialization with AWS China partition"""
        metadata = {
            'glueAccessRoleArn': 'arn:aws-cn:iam::123456789012:role/role-name',
            'region': 'NORTHERNVIRGINIA'
        }
        
        attr = AWSGlueMetadataCollibraAttribute(metadata)
        
        assert attr.account_id == '123456789012'

    def test_init_with_aws_us_gov_partition(self):
        """Test initialization with AWS GovCloud partition"""
        metadata = {
            'glueAccessRoleArn': 'arn:aws-us-gov:iam::123456789012:role/role-name',
            'region': 'NORTHERNVIRGINIA'
        }
        
        attr = AWSGlueMetadataCollibraAttribute(metadata)
        
        assert attr.account_id == '123456789012'

    def test_init_raises_error_with_invalid_arn(self):
        """Test initialization raises ValueError with invalid ARN"""
        metadata = {
            'glueAccessRoleArn': 'invalid-arn',
            'region': 'NORTHERNVIRGINIA'
        }
        
        with pytest.raises(ValueError) as exc_info:
            AWSGlueMetadataCollibraAttribute(metadata)
        
        assert "Invalid ARN or missing account ID" in str(exc_info.value)

    def test_init_raises_error_with_arn_missing_account_id(self):
        """Test initialization raises ValueError when ARN missing account ID"""
        metadata = {
            'glueAccessRoleArn': 'arn:aws:iam:::role/role-name',
            'region': 'NORTHERNVIRGINIA'
        }
        
        with pytest.raises(ValueError) as exc_info:
            AWSGlueMetadataCollibraAttribute(metadata)
        
        assert "Invalid ARN or missing account ID" in str(exc_info.value)

    def test_properties_are_read_only(self):
        """Test that properties cannot be set directly"""
        metadata = {
            'glueAccessRoleArn': 'arn:aws:iam::123456789012:role/role-name',
            'region': 'NORTHERNVIRGINIA'
        }
        attr = AWSGlueMetadataCollibraAttribute(metadata)
        
        with pytest.raises(AttributeError):
            attr.account_id = "999999999999"
        
        with pytest.raises(AttributeError):
            attr.region = "us-west-2"

    def test_init_with_different_service_in_arn(self):
        """Test initialization with different AWS service in ARN"""
        metadata = {
            'glueAccessRoleArn': 'arn:aws:s3::123456789012:bucket/my-bucket',
            'region': 'NORTHERNVIRGINIA'
        }
        
        attr = AWSGlueMetadataCollibraAttribute(metadata)
        
        assert attr.account_id == '123456789012'

    def test_init_with_unmapped_region_returns_none(self):
        """Test initialization with unmapped region returns None"""
        metadata = {
            'glueAccessRoleArn': 'arn:aws:iam::123456789012:role/role-name',
            'region': 'UNKNOWN_REGION'
        }
        
        attr = AWSGlueMetadataCollibraAttribute(metadata)
        
        assert attr.account_id == '123456789012'
        assert attr.region is None
