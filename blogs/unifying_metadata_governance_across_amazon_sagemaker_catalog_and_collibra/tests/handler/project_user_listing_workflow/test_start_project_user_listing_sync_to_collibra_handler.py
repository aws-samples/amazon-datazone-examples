"""
Unit tests for lambda/handler/project_user_listing_workflow/start_project_user_listing_sync_to_collibra_handler.py
"""
import pytest
from unittest.mock import MagicMock, patch


class MockOutput:
    """Mock output class with __dict__() method"""
    def __init__(self, data):
        self.data = data
    
    def __dict__(self):
        return self.data


@pytest.mark.unit
class TestStartProjectUserListingSyncToCollibraHandler:
    """Tests for start_project_user_listing_sync_to_collibra_handler"""

    @patch('handler.project_user_listing_workflow.start_project_user_listing_sync_to_collibra_handler.ProjectUserListingSyncBusinessLogic')
    @patch('handler.project_user_listing_workflow.start_project_user_listing_sync_to_collibra_handler.ProjectUserListingSyncWorkflowEvent')
    def test_handle_request_syncs_projects_users_listings(self, mock_event_class, mock_business_logic_class):
        """Test handle_request syncs projects, users, and listings"""
        from handler.project_user_listing_workflow.start_project_user_listing_sync_to_collibra_handler import handle_request
        
        mock_workflow_event = MagicMock()
        mock_event_class.return_value = mock_workflow_event
        
        mock_output = MockOutput({"status": "completed"})
        
        mock_business_logic = MagicMock()
        mock_business_logic.sync.return_value = mock_output
        mock_business_logic_class.return_value = mock_business_logic
        
        event = {"workflow": "project_user_listing_sync"}
        context = MagicMock()
        
        result = handle_request(event, context)
        
        assert result == {"status": "completed"}
        mock_event_class.assert_called_once_with(event)
        mock_business_logic.sync.assert_called_once_with(mock_workflow_event)

    @patch('handler.project_user_listing_workflow.start_project_user_listing_sync_to_collibra_handler.ProjectUserListingSyncBusinessLogic')
    @patch('handler.project_user_listing_workflow.start_project_user_listing_sync_to_collibra_handler.ProjectUserListingSyncWorkflowEvent')
    def test_handle_request_creates_workflow_event_from_input(self, mock_event_class, mock_business_logic_class):
        """Test handle_request creates workflow event from input event"""
        from handler.project_user_listing_workflow.start_project_user_listing_sync_to_collibra_handler import handle_request
        
        mock_workflow_event = MagicMock()
        mock_event_class.return_value = mock_workflow_event
        
        mock_output = MockOutput({})
        
        mock_business_logic = MagicMock()
        mock_business_logic.sync.return_value = mock_output
        mock_business_logic_class.return_value = mock_business_logic
        
        event = {"project_id": "proj-123"}
        context = MagicMock()
        
        handle_request(event, context)
        
        mock_event_class.assert_called_once_with(event)

    @patch('handler.project_user_listing_workflow.start_project_user_listing_sync_to_collibra_handler.ProjectUserListingSyncBusinessLogic')
    @patch('handler.project_user_listing_workflow.start_project_user_listing_sync_to_collibra_handler.ProjectUserListingSyncWorkflowEvent')
    def test_handle_request_returns_output_dict(self, mock_event_class, mock_business_logic_class):
        """Test handle_request returns output as dictionary"""
        from handler.project_user_listing_workflow.start_project_user_listing_sync_to_collibra_handler import handle_request
        
        mock_workflow_event = MagicMock()
        mock_event_class.return_value = mock_workflow_event
        
        output_dict = {
            "projects_synced": 5,
            "users_synced": 20,
            "listings_synced": 15
        }
        mock_output = MockOutput(output_dict)
        
        mock_business_logic = MagicMock()
        mock_business_logic.sync.return_value = mock_output
        mock_business_logic_class.return_value = mock_business_logic
        
        event = {}
        context = MagicMock()
        
        result = handle_request(event, context)
        
        assert result == output_dict
        assert result["projects_synced"] == 5
        assert result["users_synced"] == 20
        assert result["listings_synced"] == 15
