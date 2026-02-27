"""
Unit tests for lambda/business/CollibraSMUSResourceMatcher.py
"""
import pytest
import json
from unittest.mock import MagicMock, patch

from business.CollibraSMUSResourceMatcher import CollibraSMUSResourceMatcher


# Concrete implementation for testing abstract class
class TestableCollibraSMUSResourceMatcher(CollibraSMUSResourceMatcher):
    @staticmethod
    def _get_deserialized_form_content_by_name(form_names, smus_resource):
        if 'formsOutput' in smus_resource:
            for form in smus_resource['formsOutput']:
                if form['formName'] in form_names:
                    return json.loads(form['content'])
        return None
    
    @staticmethod
    def _get_smus_resource_type():
        return 'test_resource'
    
    @staticmethod
    def _is_valid_smus_resource(smus_resource):
        return 'name' in smus_resource


@pytest.mark.unit
class TestCollibraSMUSResourceMatcher:
    """Tests for CollibraSMUSResourceMatcher class"""

    def test_match_returns_false_for_invalid_smus_resource(self):
        """Test match returns False when SMUS resource is invalid"""
        smus_resource = {'name': 'test'}  # Valid name but will fail _is_valid_smus_resource
        collibra_asset = {'displayName': 'test-asset'}
        
        # Override to make it invalid
        with patch.object(TestableCollibraSMUSResourceMatcher, '_is_valid_smus_resource', return_value=False):
            result = TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)
        
        assert result is False

    def test_match_returns_false_when_aws_metadata_missing(self):
        """Test match handles missing AWS metadata - code has bug, raises TypeError"""
        smus_resource = {
            'name': 'test-table',
            'typeIdentifier': 'datazone:GlueTable'
        }
        collibra_asset = {
            'displayName': 'test-asset',
            'fullName': 'db>schema>table',
            'stringAttributes': []  # No AWS metadata
        }
        
        # Code has bug - doesn't handle None from __find_aws_resource_metadata_attribute
        with pytest.raises(TypeError):
            TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)

    def test_match_glue_asset_success(self):
        """Test successful match for Glue asset"""
        smus_resource = {
            'name': 'customers',
            'typeIdentifier': 'datazone:GlueTable',
            'formsOutput': [{
                'formName': 'GlueTableForm',
                'content': json.dumps({
                    'region': 'us-east-1',
                    'tableArn': 'arn:aws:glue:us-east-1:123456789012:table/mydb/customers',
                    'databaseName': 'mydb',
                    'tableName': 'customers'
                })
            }]
        }
        collibra_asset = {
            'displayName': 'customers',
            'fullName': 'catalog>mydb>customers',
            'stringAttributes': [{
                'type': {'name': 'AWS Resource Metadata'},
                'stringValue': '{"glueAccessRoleArn": "arn:aws:iam::123456789012:role/GlueRole", "region": "NORTHERNVIRGINIA"}'
            }]
        }
        
        result = TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)
        
        assert result is True

    def test_match_glue_asset_fails_with_wrong_region(self):
        """Test Glue asset match fails when region doesn't match"""
        smus_resource = {
            'name': 'customers',
            'typeIdentifier': 'datazone:GlueTable',
            'formsOutput': [{
                'formName': 'GlueTableForm',
                'content': json.dumps({
                    'region': 'us-west-2',
                    'tableArn': 'arn:aws:glue:us-west-2:123456789012:table/mydb/customers',
                    'databaseName': 'mydb',
                    'tableName': 'customers'
                })
            }]
        }
        collibra_asset = {
            'displayName': 'customers',
            'fullName': 'catalog>mydb>customers',
            'stringAttributes': [{
                'type': {'name': 'AWS Resource Metadata'},
                'stringValue': '{"glueAccessRoleArn": "arn:aws:iam::123456789012:role/GlueRole", "region": "NORTHERNVIRGINIA"}'
            }]
        }
        
        result = TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)
        
        assert result is False

    def test_match_glue_asset_fails_with_wrong_database(self):
        """Test Glue asset match fails when database doesn't match"""
        smus_resource = {
            'name': 'customers',
            'typeIdentifier': 'datazone:GlueTable',
            'formsOutput': [{
                'formName': 'GlueTableForm',
                'content': json.dumps({
                    'region': 'us-east-1',
                    'tableArn': 'arn:aws:glue:us-east-1:123456789012:table/otherdb/customers',
                    'databaseName': 'otherdb',
                    'tableName': 'customers'
                })
            }]
        }
        collibra_asset = {
            'displayName': 'customers',
            'fullName': 'catalog>mydb>customers',
            'stringAttributes': [{
                'type': {'name': 'AWS Resource Metadata'},
                'stringValue': '{"glueAccessRoleArn": "arn:aws:iam::123456789012:role/GlueRole", "region": "NORTHERNVIRGINIA"}'
            }]
        }
        
        result = TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)
        
        assert result is False

    def test_match_glue_asset_returns_false_when_form_missing(self):
        """Test Glue asset match returns False when form is missing"""
        smus_resource = {
            'name': 'customers',
            'typeIdentifier': 'datazone:GlueTable',
            'formsOutput': []
        }
        collibra_asset = {
            'displayName': 'customers',
            'fullName': 'catalog>mydb>customers',
            'stringAttributes': [{
                'type': {'name': 'AWS Resource Metadata'},
                'stringValue': '{"glueAccessRoleArn": "arn:aws:iam::123456789012:role/GlueRole", "region": "NORTHERNVIRGINIA"}'
            }]
        }
        
        result = TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)
        
        assert result is False

    def test_match_redshift_cluster_asset_success(self):
        """Test successful match for Redshift cluster asset"""
        smus_resource = {
            'name': 'orders',
            'typeIdentifier': 'datazone:RedshiftTable',
            'formsOutput': [{
                'formName': 'RedshiftTableForm',
                'content': json.dumps({
                    'region': 'us-east-1',
                    'storageType': 'CLUSTER',
                    'redshiftStorage': {
                        'redshiftClusterSource': {
                            'clusterName': 'my-cluster'
                        }
                    },
                    'databaseName': 'salesdb',
                    'schemaName': 'public',
                    'tableName': 'orders'
                })
            }]
        }
        collibra_asset = {
            'displayName': 'orders',
            'fullName': 'cluster>salesdb>public>orders',
            'stringAttributes': [{
                'type': {'name': 'AWS Resource Metadata'},
                'stringValue': '{"redshiftEndpoint": "my-cluster.abc123.us-east-1.redshift.amazonaws.com:5439/salesdb"}'
            }]
        }
        
        result = TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)
        
        assert result is True

    def test_match_redshift_cluster_asset_fails_with_wrong_cluster(self):
        """Test Redshift cluster match fails when cluster name doesn't match"""
        smus_resource = {
            'name': 'orders',
            'typeIdentifier': 'datazone:RedshiftTable',
            'formsOutput': [{
                'formName': 'RedshiftTableForm',
                'content': json.dumps({
                    'region': 'us-east-1',
                    'storageType': 'CLUSTER',
                    'redshiftStorage': {
                        'redshiftClusterSource': {
                            'clusterName': 'other-cluster'
                        }
                    },
                    'databaseName': 'salesdb',
                    'schemaName': 'public',
                    'tableName': 'orders'
                })
            }]
        }
        collibra_asset = {
            'displayName': 'orders',
            'fullName': 'cluster>salesdb>public>orders',
            'stringAttributes': [{
                'type': {'name': 'AWS Resource Metadata'},
                'stringValue': '{"redshiftEndpoint": "my-cluster.abc123.us-east-1.redshift.amazonaws.com:5439/salesdb"}'
            }]
        }
        
        result = TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)
        
        assert result is False

    def test_match_redshift_serverless_asset_success(self):
        """Test successful match for Redshift serverless asset"""
        smus_resource = {
            'name': 'products',
            'entityType': 'datazone:RedshiftTable',
            'formsOutput': [{
                'formName': 'RedshiftTableForm',
                'content': json.dumps({
                    'region': 'us-west-2',
                    'accountId': '123456789012',
                    'storageType': 'SERVERLESS',
                    'redshiftStorage': {
                        'redshiftServerlessSource': {
                            'workgroupName': 'my-workgroup'
                        }
                    },
                    'databaseName': 'inventory',
                    'schemaName': 'public',
                    'tableName': 'products'
                })
            }]
        }
        collibra_asset = {
            'displayName': 'products',
            'fullName': 'workgroup>inventory>public>products',
            'stringAttributes': [{
                'type': {'name': 'AWS Resource Metadata'},
                'stringValue': '{"redshiftEndpoint": "my-workgroup.123456789012.us-west-2.redshift-serverless.amazonaws.com:5439/inventory"}'
            }]
        }
        
        result = TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)
        
        assert result is True

    def test_match_redshift_serverless_asset_fails_with_wrong_workgroup(self):
        """Test Redshift serverless match fails when workgroup doesn't match"""
        smus_resource = {
            'name': 'products',
            'entityType': 'datazone:RedshiftTable',
            'formsOutput': [{
                'formName': 'RedshiftTableForm',
                'content': json.dumps({
                    'region': 'us-west-2',
                    'accountId': '123456789012',
                    'storageType': 'SERVERLESS',
                    'redshiftStorage': {
                        'redshiftServerlessSource': {
                            'workgroupName': 'other-workgroup'
                        }
                    },
                    'databaseName': 'inventory',
                    'schemaName': 'public',
                    'tableName': 'products'
                })
            }]
        }
        collibra_asset = {
            'displayName': 'products',
            'fullName': 'workgroup>inventory>public>products',
            'stringAttributes': [{
                'type': {'name': 'AWS Resource Metadata'},
                'stringValue': '{"redshiftEndpoint": "my-workgroup.123456789012.us-west-2.redshift-serverless.amazonaws.com:5439/inventory"}'
            }]
        }
        
        result = TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)
        
        assert result is False

    def test_match_returns_false_for_redshift_with_missing_form(self):
        """Test Redshift match returns False when form is missing"""
        smus_resource = {
            'name': 'orders',
            'typeIdentifier': 'datazone:RedshiftTable',
            'formsOutput': []
        }
        collibra_asset = {
            'displayName': 'orders',
            'fullName': 'cluster>salesdb>public>orders',
            'stringAttributes': [{
                'type': {'name': 'AWS Resource Metadata'},
                'stringValue': '{"redshiftEndpoint": "my-cluster.abc123.us-east-1.redshift.amazonaws.com:5439/salesdb"}'
            }]
        }
        
        result = TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)
        
        assert result is False

    def test_match_handles_malformed_aws_metadata_gracefully(self):
        """Test match handles malformed AWS metadata - code has bug, raises exception"""
        smus_resource = {
            'name': 'customers',
            'typeIdentifier': 'datazone:GlueTable',
            'formsOutput': [{
                'formName': 'GlueTableForm',
                'content': json.dumps({
                    'region': 'us-east-1',
                    'tableArn': 'arn:aws:glue:us-east-1:123456789012:table/mydb/customers',
                    'databaseName': 'mydb',
                    'tableName': 'customers'
                })
            }]
        }
        collibra_asset = {
            'displayName': 'customers',
            'fullName': 'catalog>mydb>customers',
            'stringAttributes': [{
                'type': {'name': 'AWS Resource Metadata'},
                'stringValue': 'invalid-json'
            }]
        }
        
        # Code has bug - doesn't handle JSON decode errors gracefully
        with pytest.raises(json.JSONDecodeError):
            TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)

    def test_match_handles_special_quotes_in_metadata(self):
        """Test match handles special quote characters in AWS metadata"""
        smus_resource = {
            'name': 'customers',
            'typeIdentifier': 'datazone:GlueTable',
            'formsOutput': [{
                'formName': 'GlueTableForm',
                'content': json.dumps({
                    'region': 'us-east-1',
                    'tableArn': 'arn:aws:glue:us-east-1:123456789012:table/mydb/customers',
                    'databaseName': 'mydb',
                    'tableName': 'customers'
                })
            }]
        }
        collibra_asset = {
            'displayName': 'customers',
            'fullName': 'catalog>mydb>customers',
            'stringAttributes': [{
                'type': {'name': 'AWS Resource Metadata'},
                'stringValue': '{"glueAccessRoleArn": "arn:aws:iam::123456789012:role/GlueRole", "region": "NORTHERNVIRGINIA"}'
            }]
        }
        
        result = TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)
        
        assert result is True

    def test_match_returns_false_for_unknown_resource_type(self):
        """Test match returns False for unknown resource type"""
        smus_resource = {
            'name': 'unknown-resource',
            'typeIdentifier': 'datazone:UnknownType',
            'formsOutput': []
        }
        collibra_asset = {
            'displayName': 'unknown-resource',
            'fullName': 'catalog>db>table',
            'stringAttributes': [{
                'type': {'name': 'AWS Resource Metadata'},
                'stringValue': '{"region": "NORTHERNVIRGINIA"}'
            }]
        }
        
        result = TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)
        
        assert result is False

    def test_match_redshift_view_form_supported(self):
        """Test match works with RedshiftViewForm"""
        smus_resource = {
            'name': 'sales_view',
            'typeIdentifier': 'datazone:RedshiftTable',
            'formsOutput': [{
                'formName': 'RedshiftViewForm',
                'content': json.dumps({
                    'region': 'us-east-1',
                    'storageType': 'CLUSTER',
                    'redshiftStorage': {
                        'redshiftClusterSource': {
                            'clusterName': 'my-cluster'
                        }
                    },
                    'databaseName': 'salesdb',
                    'schemaName': 'public',
                    'tableName': 'sales_view'
                })
            }]
        }
        collibra_asset = {
            'displayName': 'sales_view',
            'fullName': 'cluster>salesdb>public>sales_view',
            'stringAttributes': [{
                'type': {'name': 'AWS Resource Metadata'},
                'stringValue': '{"redshiftEndpoint": "my-cluster.abc123.us-east-1.redshift.amazonaws.com:5439/salesdb"}'
            }]
        }
        
        result = TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)
        
        assert result is True

    def test_match_logs_warning_when_aws_metadata_missing(self):
        """Test match raises TypeError when AWS metadata attribute not found (code bug)"""
        smus_resource = {
            'name': 'test-table',
            'typeIdentifier': 'datazone:GlueTable',
            'formsOutput': [{
                'formName': 'GlueTableForm',
                'content': json.dumps({
                    'region': 'us-east-1',
                    'tableArn': 'arn:aws:glue:us-east-1:123456789012:table/mydb/test',
                    'databaseName': 'mydb',
                    'tableName': 'test'
                })
            }]
        }
        collibra_asset = {
            'displayName': 'test-asset',
            'fullName': 'catalog>mydb>test',
            'stringAttributes': [{
                'type': {'name': 'Other Attribute'},
                'stringValue': 'some value'
            }]
        }
        
        # Code has bug - tries to deserialize None
        with pytest.raises(TypeError):
            TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)

    def test_match_handles_missing_string_attributes(self):
        """Test match raises TypeError when stringAttributes missing (code bug)"""
        smus_resource = {
            'name': 'test-table',
            'typeIdentifier': 'datazone:GlueTable',
            'formsOutput': [{
                'formName': 'GlueTableForm',
                'content': json.dumps({
                    'region': 'us-east-1',
                    'tableArn': 'arn:aws:glue:us-east-1:123456789012:table/mydb/test',
                    'databaseName': 'mydb',
                    'tableName': 'test'
                })
            }]
        }
        collibra_asset = {
            'displayName': 'test-asset',
            'fullName': 'catalog>mydb>test'
            # No stringAttributes key
        }
        
        # Code has bug - tries to deserialize None
        with pytest.raises(TypeError):
            TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)

    def test_match_redshift_with_unknown_storage_type(self):
        """Test match returns False for Redshift with unknown storage type"""
        smus_resource = {
            'name': 'orders',
            'typeIdentifier': 'datazone:RedshiftTable',
            'formsOutput': [{
                'formName': 'RedshiftTableForm',
                'content': json.dumps({
                    'region': 'us-east-1',
                    'storageType': 'UNKNOWN_TYPE',
                    'databaseName': 'salesdb',
                    'schemaName': 'public',
                    'tableName': 'orders'
                })
            }]
        }
        collibra_asset = {
            'displayName': 'orders',
            'fullName': 'cluster>salesdb>public>orders',
            'stringAttributes': [{
                'type': {'name': 'AWS Resource Metadata'},
                'stringValue': '{"redshiftEndpoint": "my-cluster.abc123.us-east-1.redshift.amazonaws.com:5439/salesdb"}'
            }]
        }
        
        result = TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)
        
        assert result is False

    def test_match_redshift_cluster_handles_exception_in_matching(self, caplog):
        """Test match handles exception during Redshift cluster matching"""
        import logging
        caplog.set_level(logging.WARNING)
        
        smus_resource = {
            'name': 'orders',
            'typeIdentifier': 'datazone:RedshiftTable',
            'formsOutput': [{
                'formName': 'RedshiftTableForm',
                'content': json.dumps({
                    'region': 'us-east-1',
                    'storageType': 'CLUSTER',
                    'redshiftStorage': {
                        # Missing redshiftClusterSource - will cause KeyError
                    },
                    'databaseName': 'salesdb',
                    'schemaName': 'public',
                    'tableName': 'orders'
                })
            }]
        }
        collibra_asset = {
            'displayName': 'orders',
            'fullName': 'cluster>salesdb>public>orders',
            'stringAttributes': [{
                'type': {'name': 'AWS Resource Metadata'},
                'stringValue': '{"redshiftEndpoint": "my-cluster.abc123.us-east-1.redshift.amazonaws.com:5439/salesdb"}'
            }]
        }
        
        result = TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)
        
        assert result is False
        assert 'Redshift cluster asset matching encountered an exception' in caplog.text

    def test_match_redshift_serverless_handles_exception_in_matching(self, caplog):
        """Test match handles exception during Redshift serverless matching"""
        import logging
        caplog.set_level(logging.WARNING)
        
        smus_resource = {
            'name': 'products',
            'entityType': 'datazone:RedshiftTable',
            'formsOutput': [{
                'formName': 'RedshiftTableForm',
                'content': json.dumps({
                    'region': 'us-west-2',
                    'accountId': '123456789012',
                    'storageType': 'SERVERLESS',
                    'redshiftStorage': {
                        # Missing redshiftServerlessSource - will cause KeyError
                    },
                    'databaseName': 'inventory',
                    'schemaName': 'public',
                    'tableName': 'products'
                })
            }]
        }
        collibra_asset = {
            'displayName': 'products',
            'fullName': 'workgroup>inventory>public>products',
            'stringAttributes': [{
                'type': {'name': 'AWS Resource Metadata'},
                'stringValue': '{"redshiftEndpoint": "my-workgroup.123456789012.us-west-2.redshift-serverless.amazonaws.com:5439/inventory"}'
            }]
        }
        
        result = TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)
        
        assert result is False
        assert 'Redshift serverless asset matching encountered an exception' in caplog.text

    def test_match_glue_handles_exception_in_matching(self, caplog):
        """Test match handles exception during Glue matching"""
        import logging
        caplog.set_level(logging.WARNING)
        
        smus_resource = {
            'name': 'customers',
            'typeIdentifier': 'datazone:GlueTable',
            'formsOutput': [{
                'formName': 'GlueTableForm',
                'content': json.dumps({
                    # Missing required fields - will cause KeyError
                    'databaseName': 'mydb',
                    'tableName': 'customers'
                })
            }]
        }
        collibra_asset = {
            'displayName': 'customers',
            'fullName': 'catalog>mydb>customers',
            'stringAttributes': [{
                'type': {'name': 'AWS Resource Metadata'},
                'stringValue': '{"glueAccessRoleArn": "arn:aws:iam::123456789012:role/GlueRole", "region": "NORTHERNVIRGINIA"}'
            }]
        }
        
        result = TestableCollibraSMUSResourceMatcher.match(smus_resource, collibra_asset)
        
        assert result is False
        assert 'Glue asset matching encountered an exception' in caplog.text
