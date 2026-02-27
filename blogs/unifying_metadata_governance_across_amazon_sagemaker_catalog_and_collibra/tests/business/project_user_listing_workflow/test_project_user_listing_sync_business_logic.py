"""
Unit tests for lambda/business/project_user_listing_workflow/ProjectUserListingSyncBusinessLogic.py
"""
import pytest
from unittest.mock import MagicMock, patch

from business.project_user_listing_workflow.ProjectUserListingSyncBusinessLogic import ProjectUserListingSyncBusinessLogic
from model.ProjectUserListingSyncWorkflowEvent import ProjectUserListingSyncWorkflowEvent


@pytest.mark.unit
class TestProjectUserListingSyncBusinessLogic:
    """Tests for ProjectUserListingSyncBusinessLogic class"""

    @pytest.fixture
    def mock_smus_adapter(self):
        """Mock SMUS adapter"""
        return MagicMock()

    @pytest.fixture
    def mock_collibra_adapter(self):
        """Mock Collibra adapter"""
        return MagicMock()

    @pytest.fixture
    def business_logic(self, mock_logger, mock_smus_adapter, mock_collibra_adapter):
        """Create ProjectUserListingSyncBusinessLogic instance with mocked dependencies"""
        with patch('business.project_user_listing_workflow.ProjectUserListingSyncBusinessLogic.SMUSAdapter', return_value=mock_smus_adapter):
            with patch('business.project_user_listing_workflow.ProjectUserListingSyncBusinessLogic.CollibraAdapter', return_value=mock_collibra_adapter):
                return ProjectUserListingSyncBusinessLogic(mock_logger)

    def test_sync_processes_projects(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test sync processes projects from SMUS"""
        mock_smus_adapter.list_projects.return_value = {
            'items': [
                {'id': 'proj-1', 'name': 'Project1'},
                {'id': 'proj-2', 'name': 'Project2'}
            ]
        }
        mock_smus_adapter.get_project.return_value = {'name': 'Project1'}
        mock_collibra_adapter.get_or_create_aws_project.return_value = {'id': 'collibra-proj-1'}
        mock_smus_adapter.search_all_listings.return_value = []
        mock_smus_adapter.list_all_users_in_project.return_value = []
        
        event = ProjectUserListingSyncWorkflowEvent({'next_project_token': None})
        
        result = business_logic.sync(event)
        
        assert mock_smus_adapter.list_projects.call_count == 1
        assert mock_smus_adapter.get_project.call_count == 2
        mock_logger.info.assert_any_call("Syncing 2 projects")

    def test_sync_handles_pagination(self, business_logic, mock_smus_adapter, mock_collibra_adapter):
        """Test sync handles pagination token"""
        mock_smus_adapter.list_projects.return_value = {
            'items': [{'id': 'proj-1', 'name': 'Project1'}],
            'nextToken': 'next-token-123'
        }
        mock_smus_adapter.get_project.return_value = {'name': 'Project1'}
        mock_collibra_adapter.get_or_create_aws_project.return_value = {'id': 'collibra-proj-1'}
        mock_smus_adapter.search_all_listings.return_value = []
        mock_smus_adapter.list_all_users_in_project.return_value = []
        
        event = ProjectUserListingSyncWorkflowEvent({'next_project_token': 'old-token'})
        
        result = business_logic.sync(event)
        
        assert result.next_project_token == 'next-token-123'
        mock_smus_adapter.list_projects.assert_called_once_with(5, 'old-token')

    def test_sync_handles_project_sync_failure(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test sync handles individual project sync failures gracefully"""
        mock_smus_adapter.list_projects.return_value = {
            'items': [{'id': 'proj-1', 'name': 'Project1'}]
        }
        mock_smus_adapter.get_project.side_effect = Exception("Project not found")
        
        event = ProjectUserListingSyncWorkflowEvent({'next_project_token': None})
        
        result = business_logic.sync(event)
        
        # Should not raise, should log warning
        mock_logger.warn.assert_called()

    def test_sync_project_creates_collibra_project(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test sync_project creates project in Collibra"""
        mock_smus_adapter.get_project.return_value = {'name': 'TestProject'}
        mock_collibra_adapter.get_or_create_aws_project.return_value = {'id': 'collibra-proj-1'}
        mock_smus_adapter.search_all_listings.return_value = []
        mock_smus_adapter.list_all_users_in_project.return_value = []
        
        business_logic.sync_project('proj-123')
        
        mock_collibra_adapter.get_or_create_aws_project.assert_called_once_with('TestProject', 'proj-123')
        mock_collibra_adapter.add_aws_project_attributes.assert_called_once_with('collibra-proj-1', 'proj-123')
        mock_logger.info.assert_any_call("Successfully synced project with id proj-123 and name TestProject to Collibra")

    def test_associate_project_with_listings(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test associate_project_with_listings creates relations"""
        mock_smus_adapter.search_all_listings.return_value = [
            {'assetListing': {'name': 'customers_table'}},
            {'assetListing': {'name': 'orders_table'}}
        ]
        mock_collibra_adapter.get_table_by_name.side_effect = [
            {'id': 'table-1'},
            {'id': 'table-2'}
        ]
        collibra_project = {'id': 'proj-1'}
        
        business_logic.associate_project_with_listings('smus-proj-1', collibra_project)
        
        assert mock_collibra_adapter.create_relation.call_count == 2
        mock_logger.info.assert_any_call("Successfully associated project proj-1 with asset table-1")

    def test_associate_project_with_listings_skips_missing_tables(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test associate_project_with_listings skips tables not in Collibra"""
        mock_smus_adapter.search_all_listings.return_value = [
            {'assetListing': {'name': 'nonexistent_table'}}
        ]
        mock_collibra_adapter.get_table_by_name.side_effect = Exception("Table not found")
        collibra_project = {'id': 'proj-1'}
        
        business_logic.associate_project_with_listings('smus-proj-1', collibra_project)
        
        mock_collibra_adapter.create_relation.assert_not_called()
        mock_logger.warn.assert_any_call("Asset with name nonexistent_table doesn't exist in Collibra. Skipping.")

    def test_associate_project_with_listings_handles_relation_creation_failure(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test associate_project_with_listings handles relation creation failures"""
        mock_smus_adapter.search_all_listings.return_value = [
            {'assetListing': {'name': 'customers_table'}}
        ]
        mock_collibra_adapter.get_table_by_name.return_value = {'id': 'table-1'}
        mock_collibra_adapter.create_relation.side_effect = Exception("Relation already exists")
        collibra_project = {'id': 'proj-1'}
        
        business_logic.associate_project_with_listings('smus-proj-1', collibra_project)
        
        mock_logger.warn.assert_called()

    def test_sync_users_and_associate_with_projects(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test sync_users_and_associate_with_projects syncs SSO users"""
        mock_smus_adapter.list_all_users_in_project.return_value = [
            {'memberDetails': {'user': {'userId': 'user-1'}}}
        ]
        mock_smus_adapter.get_user_profile.return_value = {
            'type': 'SSO',
            'details': {'sso': {'username': 'testuser'}}
        }
        mock_collibra_adapter.get_or_create_aws_user.return_value = {
            'id': 'collibra-user-1'
        }
        
        business_logic.sync_users_and_associate_with_projects('proj-1', 'TestProject')
        
        mock_collibra_adapter.get_or_create_aws_user.assert_called_once_with('testuser')
        mock_collibra_adapter.add_aws_user_attributes.assert_called_once_with('collibra-user-1', 'TestProject')

    def test_sync_users_skips_iam_users(self, business_logic, mock_smus_adapter, mock_collibra_adapter):
        """Test sync_users_and_associate_with_projects skips IAM users"""
        mock_smus_adapter.list_all_users_in_project.return_value = [
            {'memberDetails': {'user': {'userId': 'user-1'}}}
        ]
        mock_smus_adapter.get_user_profile.return_value = {
            'type': 'IAM'
        }
        
        business_logic.sync_users_and_associate_with_projects('proj-1', 'TestProject')
        
        mock_collibra_adapter.get_or_create_aws_user.assert_not_called()

    def test_sync_users_skips_adding_duplicate_project_attribute(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test sync_users_and_associate_with_projects skips duplicate project attributes"""
        mock_smus_adapter.list_all_users_in_project.return_value = [
            {'memberDetails': {'user': {'userId': 'user-1'}}}
        ]
        mock_smus_adapter.get_user_profile.return_value = {
            'type': 'SSO',
            'details': {'sso': {'username': 'testuser'}}
        }
        mock_collibra_adapter.get_or_create_aws_user.return_value = {
            'id': 'collibra-user-1',
            'stringAttributes': [
                {'stringValue': 'TestProject'}  # Already has this project
            ]
        }
        
        business_logic.sync_users_and_associate_with_projects('proj-1', 'TestProject')
        
        mock_collibra_adapter.add_aws_user_attributes.assert_not_called()
        mock_logger.info.assert_any_call("Should add project attribute is False for user testuser")

    def test_sync_users_handles_user_sync_failure(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test sync_users_and_associate_with_projects handles user sync failures"""
        mock_smus_adapter.list_all_users_in_project.return_value = [
            {'memberDetails': {'user': {'userId': 'user-1'}}}
        ]
        mock_smus_adapter.get_user_profile.side_effect = Exception("User not found")
        
        business_logic.sync_users_and_associate_with_projects('proj-1', 'TestProject')
        
        mock_logger.warn.assert_called()

    def test_sync_with_empty_projects_list(self, business_logic, mock_smus_adapter, mock_logger):
        """Test sync handles empty projects list"""
        mock_smus_adapter.list_projects.return_value = {'items': []}
        
        event = ProjectUserListingSyncWorkflowEvent({'next_project_token': None})
        
        result = business_logic.sync(event)
        
        mock_logger.info.assert_any_call("Syncing 0 projects")
