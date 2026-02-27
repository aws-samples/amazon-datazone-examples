"""
Unit tests for lambda/business/CollibraSMUSAssetMatcher.py
"""
import pytest
import json

from business.CollibraSMUSAssetMatcher import CollibraSMUSAssetMatcher


@pytest.mark.unit
class TestCollibraSMUSAssetMatcher:
    """Tests for CollibraSMUSAssetMatcher class"""

    def test_get_deserialized_form_content_by_name_returns_content(self):
        """Test _get_deserialized_form_content_by_name returns deserialized form"""
        smus_asset = {
            'formsOutput': [
                {
                    'formName': 'GlueTableForm',
                    'content': json.dumps({'database': 'mydb', 'table': 'mytable'})
                }
            ]
        }
        
        result = CollibraSMUSAssetMatcher._get_deserialized_form_content_by_name(
            ['GlueTableForm'], smus_asset
        )
        
        assert result['database'] == 'mydb'
        assert result['table'] == 'mytable'

    def test_get_deserialized_form_content_by_name_returns_none_when_not_found(self):
        """Test _get_deserialized_form_content_by_name returns None when form not found"""
        smus_asset = {
            'formsOutput': [
                {
                    'formName': 'OtherForm',
                    'content': json.dumps({'data': 'value'})
                }
            ]
        }
        
        result = CollibraSMUSAssetMatcher._get_deserialized_form_content_by_name(
            ['GlueTableForm'], smus_asset
        )
        
        assert result is None

    def test_get_deserialized_form_content_by_name_checks_multiple_forms(self):
        """Test _get_deserialized_form_content_by_name checks multiple form names"""
        smus_asset = {
            'formsOutput': [
                {
                    'formName': 'RedshiftTableForm',
                    'content': json.dumps({'cluster': 'mycluster'})
                }
            ]
        }
        
        result = CollibraSMUSAssetMatcher._get_deserialized_form_content_by_name(
            ['GlueTableForm', 'RedshiftTableForm'], smus_asset
        )
        
        assert result['cluster'] == 'mycluster'



    def test_is_valid_smus_resource_returns_true_with_external_identifier(self):
        """Test _is_valid_smus_resource returns True when externalIdentifier present"""
        smus_resource = {'externalIdentifier': 'arn:aws:glue:us-east-1:123456789012:table/db/table'}
        
        result = CollibraSMUSAssetMatcher._is_valid_smus_resource(smus_resource)
        
        assert result is True

    def test_is_valid_smus_resource_returns_false_without_external_identifier(self):
        """Test _is_valid_smus_resource returns False when externalIdentifier missing"""
        smus_resource = {'name': 'mytable'}
        
        result = CollibraSMUSAssetMatcher._is_valid_smus_resource(smus_resource)
        
        assert result is False
