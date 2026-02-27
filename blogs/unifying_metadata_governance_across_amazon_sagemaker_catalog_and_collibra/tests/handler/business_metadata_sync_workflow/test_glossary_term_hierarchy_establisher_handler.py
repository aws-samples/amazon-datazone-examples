"""
Unit tests for lambda/handler/business_metadata_sync_workflow/glossary_term_hierarchy_establisher_handler.py
"""
import pytest
from unittest.mock import MagicMock, patch


@pytest.mark.unit
class TestGlossaryTermHierarchyEstablisherHandler:
    """Tests for glossary_term_hierarchy_establisher_handler"""

    @patch('handler.business_metadata_sync_workflow.glossary_term_hierarchy_establisher_handler.GlossaryTermHierarchyEstablisherBusinessLogic')
    def test_handle_request_establishes_hierarchy(self, mock_business_logic_class):
        """Test handle_request establishes glossary term hierarchy"""
        from handler.business_metadata_sync_workflow.glossary_term_hierarchy_establisher_handler import handle_request
        
        mock_business_logic = MagicMock()
        mock_business_logic_class.return_value = mock_business_logic
        
        event = {"workflow": "business_metadata_sync"}
        context = MagicMock()
        
        result = handle_request(event, context)
        
        assert result == event
        mock_business_logic.establish.assert_called_once()

    @patch('handler.business_metadata_sync_workflow.glossary_term_hierarchy_establisher_handler.GlossaryTermHierarchyEstablisherBusinessLogic')
    def test_handle_request_calls_establish_without_parameters(self, mock_business_logic_class):
        """Test handle_request calls establish without parameters"""
        from handler.business_metadata_sync_workflow.glossary_term_hierarchy_establisher_handler import handle_request
        
        mock_business_logic = MagicMock()
        mock_business_logic_class.return_value = mock_business_logic
        
        event = {}
        context = MagicMock()
        
        handle_request(event, context)
        
        mock_business_logic.establish.assert_called_once_with()

    @patch('handler.business_metadata_sync_workflow.glossary_term_hierarchy_establisher_handler.GlossaryTermHierarchyEstablisherBusinessLogic')
    def test_handle_request_returns_original_event(self, mock_business_logic_class):
        """Test handle_request returns the original event"""
        from handler.business_metadata_sync_workflow.glossary_term_hierarchy_establisher_handler import handle_request
        
        mock_business_logic = MagicMock()
        mock_business_logic_class.return_value = mock_business_logic
        
        event = {"step": "hierarchy_establishment"}
        context = MagicMock()
        
        result = handle_request(event, context)
        
        assert result is event
