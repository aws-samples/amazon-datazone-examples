"""
Unit tests for lambda/business/business_metadata_sync_workflow/GlossarySyncBusinessLogic.py
"""
import pytest
from unittest.mock import MagicMock, patch

from business.business_metadata_sync_workflow.GlossarySyncBusinessLogic import GlossarySyncBusinessLogic


@pytest.mark.unit
class TestGlossarySyncBusinessLogic:
    """Tests for GlossarySyncBusinessLogic class"""

    @pytest.fixture
    def mock_smus_adapter(self):
        """Mock SMUS adapter"""
        adapter = MagicMock()
        adapter.create_or_get_glossary.return_value = 'glossary-123'
        return adapter

    @pytest.fixture
    def mock_collibra_adapter(self):
        """Mock Collibra adapter"""
        return MagicMock()

    @pytest.fixture
    def business_logic(self, mock_logger, mock_smus_adapter, mock_collibra_adapter):
        """Create GlossarySyncBusinessLogic instance with mocked dependencies"""
        with patch('business.business_metadata_sync_workflow.GlossarySyncBusinessLogic.SMUSAdapter', return_value=mock_smus_adapter):
            with patch('business.business_metadata_sync_workflow.GlossarySyncBusinessLogic.CollibraAdapter', return_value=mock_collibra_adapter):
                return GlossarySyncBusinessLogic(mock_logger)

    def test_init_creates_glossary(self, mock_logger, mock_smus_adapter, mock_collibra_adapter):
        """Test initialization creates or gets glossary"""
        with patch('business.business_metadata_sync_workflow.GlossarySyncBusinessLogic.SMUSAdapter', return_value=mock_smus_adapter):
            with patch('business.business_metadata_sync_workflow.GlossarySyncBusinessLogic.CollibraAdapter', return_value=mock_collibra_adapter):
                logic = GlossarySyncBusinessLogic(mock_logger)
                
                mock_smus_adapter.create_or_get_glossary.assert_called_once()

    def test_sync_creates_new_terms(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test sync creates new glossary terms"""
        mock_collibra_adapter.get_business_term_metadata.return_value = [
            {
                'id': 'term-1',
                'displayName': 'Customer ID',
                'stringAttributes': [{'stringValue': 'Unique customer identifier'}]
            }
        ]
        mock_smus_adapter.search_glossary_term_by_name.return_value = None
        
        result = business_logic.sync(None)
        
        assert result == 'term-1'
        mock_smus_adapter.create_glossary_term.assert_called_once_with(
            'glossary-123', 'Customer ID', ['Unique customer identifier']
        )
        mock_logger.info.assert_any_call("Creating glossary term 'Customer ID' with descriptions '['Unique customer identifier']'")

    def test_sync_updates_existing_term_with_changed_short_description(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test sync updates existing term when short description changes"""
        mock_collibra_adapter.get_business_term_metadata.return_value = [
            {
                'id': 'term-1',
                'displayName': 'Customer ID',
                'stringAttributes': [{'stringValue': 'Updated description'}]
            }
        ]
        mock_smus_adapter.search_glossary_term_by_name.return_value = {
            'id': 'smus-term-1',
            'shortDescription': 'Old description'
        }
        
        result = business_logic.sync(None)
        
        assert result == 'term-1'
        mock_smus_adapter.update_glossary_term_description.assert_called_once_with(
            'smus-term-1', ['Updated description']
        )
        mock_logger.info.assert_any_call("Updating glossary term 'Customer ID' with descriptions '['Updated description']'")

    def test_sync_updates_existing_term_with_changed_long_description(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test sync updates existing term when long description changes"""
        mock_collibra_adapter.get_business_term_metadata.return_value = [
            {
                'id': 'term-1',
                'displayName': 'Customer ID',
                'stringAttributes': [
                    {'stringValue': 'Description 1'},
                    {'stringValue': 'Description 2'}
                ]
            }
        ]
        mock_smus_adapter.search_glossary_term_by_name.return_value = {
            'id': 'smus-term-1',
            'longDescription': 'Old description'
        }
        
        result = business_logic.sync(None)
        
        assert result == 'term-1'
        mock_smus_adapter.update_glossary_term_description.assert_called_once()

    def test_sync_skips_update_when_description_unchanged(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test sync skips update when description hasn't changed"""
        mock_collibra_adapter.get_business_term_metadata.return_value = [
            {
                'id': 'term-1',
                'displayName': 'Customer ID',
                'stringAttributes': [{'stringValue': 'Same description'}]
            }
        ]
        mock_smus_adapter.search_glossary_term_by_name.return_value = {
            'id': 'smus-term-1',
            'shortDescription': 'Same description'
        }
        
        result = business_logic.sync(None)
        
        assert result == 'term-1'
        mock_smus_adapter.update_glossary_term_description.assert_not_called()
        mock_logger.info.assert_any_call("Update to glossary term 'Customer ID' not required.")

    def test_sync_handles_multiple_terms(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test sync handles multiple glossary terms"""
        mock_collibra_adapter.get_business_term_metadata.return_value = [
            {
                'id': 'term-1',
                'displayName': 'Customer ID',
                'stringAttributes': [{'stringValue': 'Customer identifier'}]
            },
            {
                'id': 'term-2',
                'displayName': 'Order Date',
                'stringAttributes': [{'stringValue': 'Order date'}]
            }
        ]
        mock_smus_adapter.search_glossary_term_by_name.return_value = None
        
        result = business_logic.sync(None)
        
        assert result == 'term-2'
        assert mock_smus_adapter.create_glossary_term.call_count == 2
        mock_logger.info.assert_any_call("Found 2 terms in Collibra.")

    def test_sync_skips_duplicate_term_names(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test sync skips duplicate term names in same batch"""
        mock_collibra_adapter.get_business_term_metadata.return_value = [
            {
                'id': 'term-1',
                'displayName': 'Customer ID',
                'stringAttributes': [{'stringValue': 'First'}]
            },
            {
                'id': 'term-2',
                'displayName': 'Customer ID',  # Duplicate name
                'stringAttributes': [{'stringValue': 'Second'}]
            }
        ]
        mock_smus_adapter.search_glossary_term_by_name.return_value = None
        
        result = business_logic.sync(None)
        
        assert result == 'term-2'
        # Should only create once
        assert mock_smus_adapter.create_glossary_term.call_count == 1

    def test_sync_with_last_seen_id(self, business_logic, mock_smus_adapter, mock_collibra_adapter):
        """Test sync passes last_seen_id to Collibra adapter"""
        mock_collibra_adapter.get_business_term_metadata.return_value = []
        
        business_logic.sync('last-term-123')
        
        mock_collibra_adapter.get_business_term_metadata.assert_called_once_with('last-term-123')

    def test_sync_returns_none_when_no_terms(self, business_logic, mock_smus_adapter, mock_collibra_adapter):
        """Test sync returns None when no terms found"""
        mock_collibra_adapter.get_business_term_metadata.return_value = []
        
        result = business_logic.sync(None)
        
        assert result is None

    def test_sync_updates_term_when_no_existing_description(self, business_logic, mock_smus_adapter, mock_collibra_adapter):
        """Test sync updates term when existing term has no description"""
        mock_collibra_adapter.get_business_term_metadata.return_value = [
            {
                'id': 'term-1',
                'displayName': 'Customer ID',
                'stringAttributes': [{'stringValue': 'New description'}]
            }
        ]
        mock_smus_adapter.search_glossary_term_by_name.return_value = {
            'id': 'smus-term-1'
            # No shortDescription or longDescription
        }
        
        result = business_logic.sync(None)
        
        assert result == 'term-1'
        mock_smus_adapter.update_glossary_term_description.assert_called_once()

    def test_sync_handles_empty_descriptions(self, business_logic, mock_smus_adapter, mock_collibra_adapter):
        """Test sync handles terms with no descriptions"""
        mock_collibra_adapter.get_business_term_metadata.return_value = [
            {
                'id': 'term-1',
                'displayName': 'Customer ID',
                'stringAttributes': []
            }
        ]
        mock_smus_adapter.search_glossary_term_by_name.return_value = None
        
        result = business_logic.sync(None)
        
        assert result == 'term-1'
        mock_smus_adapter.create_glossary_term.assert_called_once_with(
            'glossary-123', 'Customer ID', []
        )
