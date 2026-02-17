"""
Unit tests for lambda/model/CollibraAssetType.py
"""
import pytest

from model.CollibraAssetType import CollibraAssetType


@pytest.mark.unit
class TestCollibraAssetType:
    """Tests for CollibraAssetType enum"""

    def test_business_term_value(self):
        """Test BUSINESS_TERM enum value"""
        assert CollibraAssetType.BUSINESS_TERM == "BusinessTerm"
        assert CollibraAssetType.BUSINESS_TERM.value == "BusinessTerm"

    def test_table_value(self):
        """Test TABLE enum value"""
        assert CollibraAssetType.TABLE == "Table"
        assert CollibraAssetType.TABLE.value == "Table"

    def test_enum_members(self):
        """Test that enum has exactly two members"""
        members = list(CollibraAssetType)
        assert len(members) == 2
        assert CollibraAssetType.BUSINESS_TERM in members
        assert CollibraAssetType.TABLE in members

    def test_enum_is_string(self):
        """Test that enum values are strings"""
        assert isinstance(CollibraAssetType.BUSINESS_TERM, str)
        assert isinstance(CollibraAssetType.TABLE, str)

    def test_enum_comparison(self):
        """Test enum comparison with strings"""
        assert CollibraAssetType.BUSINESS_TERM == "BusinessTerm"
        assert CollibraAssetType.TABLE == "Table"
        assert CollibraAssetType.BUSINESS_TERM != "Table"
        assert CollibraAssetType.TABLE != "BusinessTerm"

    def test_enum_in_string_operations(self):
        """Test that enum can be used in string operations"""
        asset_type = CollibraAssetType.BUSINESS_TERM
        # Enum inherits from str, so string operations work on the value
        assert asset_type.lower() == "businessterm"
        assert asset_type.upper() == "BUSINESSTERM"
        assert "Business" in asset_type
