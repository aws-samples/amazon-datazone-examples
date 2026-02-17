"""
Unit tests for lambda/business/CollibraSMUSListingMatcher.py
"""
import pytest
import json

from business.CollibraSMUSListingMatcher import CollibraSMUSListingMatcher


@pytest.mark.unit
class TestCollibraSMUSListingMatcher:
    """Tests for CollibraSMUSListingMatcher class"""

    def test_get_deserialized_form_content_by_name_returns_content(self):
        """Test _get_deserialized_form_content_by_name returns deserialized form"""
        smus_listing = {
            'additionalAttributes': {
                'forms': json.dumps({
                    'GlueTableForm': {'database': 'mydb', 'table': 'mytable'}
                })
            }
        }
        
        result = CollibraSMUSListingMatcher._get_deserialized_form_content_by_name(
            ['GlueTableForm'], smus_listing
        )
        
        assert result['database'] == 'mydb'
        assert result['table'] == 'mytable'

    def test_get_deserialized_form_content_by_name_returns_none_when_not_found(self):
        """Test _get_deserialized_form_content_by_name returns None when form not found"""
        smus_listing = {
            'additionalAttributes': {
                'forms': json.dumps({
                    'OtherForm': {'data': 'value'}
                })
            }
        }
        
        result = CollibraSMUSListingMatcher._get_deserialized_form_content_by_name(
            ['GlueTableForm'], smus_listing
        )
        
        assert result is None

    def test_get_deserialized_form_content_by_name_returns_none_without_additional_attributes(self):
        """Test _get_deserialized_form_content_by_name returns None without additionalAttributes"""
        smus_listing = {'name': 'mylisting'}
        
        result = CollibraSMUSListingMatcher._get_deserialized_form_content_by_name(
            ['GlueTableForm'], smus_listing
        )
        
        assert result is None

    def test_get_deserialized_form_content_by_name_returns_none_without_forms(self):
        """Test _get_deserialized_form_content_by_name returns None without forms"""
        smus_listing = {
            'additionalAttributes': {
                'other': 'data'
            }
        }
        
        result = CollibraSMUSListingMatcher._get_deserialized_form_content_by_name(
            ['GlueTableForm'], smus_listing
        )
        
        assert result is None

    def test_get_smus_resource_type_returns_listing(self):
        """Test _get_smus_resource_type returns 'listing'"""
        result = CollibraSMUSListingMatcher._get_smus_resource_type()
        assert result == 'listing'

    def test_is_valid_smus_resource_always_returns_true(self):
        """Test _is_valid_smus_resource always returns True"""
        assert CollibraSMUSListingMatcher._is_valid_smus_resource({}) is True
        assert CollibraSMUSListingMatcher._is_valid_smus_resource({'name': 'test'}) is True
        assert CollibraSMUSListingMatcher._is_valid_smus_resource(None) is True


