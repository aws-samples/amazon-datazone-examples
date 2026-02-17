"""
Unit tests for lambda/handler/business_metadata_sync_workflow/glossary_sync_handler.py
"""
import pytest
from unittest.mock import MagicMock, patch

from handler.business_metadata_sync_workflow import glossary_sync_handler


@pytest.mark.unit
class TestGlossarySyncHandler:
    """Tests for glossary_sync_handler"""

    @patch('handler.business_metadata_sync_workflow.glossary_sync_handler.GlossarySyncBusinessLogic')
    def test_handle_request_with_last_seen_id(self, mock_business_logic_class):
        """Test handle_request with last_seen_id in event"""
        mock_logic = MagicMock()
        mock_logic.sync.return_value = 'new-last-seen-id'
        mock_business_logic_class.return_value = mock_logic
        
        event = {'last_seen_glossary_term_id': 'old-id'}
        context = {}
        
        result = glossary_sync_handler.handle_request(event, context)
        
        assert result['last_seen_glossary_term_id'] == 'new-last-seen-id'
        mock_logic.sync.assert_called_once_with('old-id')

    @patch('handler.business_metadata_sync_workflow.glossary_sync_handler.GlossarySyncBusinessLogic')
    def test_handle_request_without_last_seen_id(self, mock_business_logic_class):
        """Test handle_request without last_seen_id in event"""
        mock_logic = MagicMock()
        mock_logic.sync.return_value = 'first-id'
        mock_business_logic_class.return_value = mock_logic
        
        event = {}
        context = {}
        
        result = glossary_sync_handler.handle_request(event, context)
        
        assert result['last_seen_glossary_term_id'] == 'first-id'
        mock_logic.sync.assert_called_once_with(None)

    @patch('handler.business_metadata_sync_workflow.glossary_sync_handler.GlossarySyncBusinessLogic')
    def test_handle_request_returns_none_when_no_more_terms(self, mock_business_logic_class):
        """Test handle_request when sync returns None"""
        mock_logic = MagicMock()
        mock_logic.sync.return_value = None
        mock_business_logic_class.return_value = mock_logic
        
        event = {}
        context = {}
        
        result = glossary_sync_handler.handle_request(event, context)
        
        assert result['last_seen_glossary_term_id'] is None
