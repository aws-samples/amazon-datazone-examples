"""
Unit tests for lambda/business/SubscriptionSyncBusinessLogic.py
"""
import pytest
from unittest.mock import MagicMock, patch

from business.SubscriptionSyncBusinessLogic import SubscriptionSyncBusinessLogic


@pytest.mark.unit
class TestSubscriptionSyncBusinessLogic:
    """Tests for SubscriptionSyncBusinessLogic class"""

    @pytest.fixture
    def mock_smus_adapter(self):
        """Mock SMUS adapter"""
        adapter = MagicMock()
        adapter.list_all_projects.return_value = [
            {'id': 'proj-1'},
            {'id': 'proj-2'}
        ]
        return adapter

    @pytest.fixture
    def mock_collibra_adapter(self):
        """Mock Collibra adapter"""
        return MagicMock()

    @pytest.fixture
    def business_logic(self, mock_logger, mock_smus_adapter, mock_collibra_adapter):
        """Create SubscriptionSyncBusinessLogic instance with mocked dependencies"""
        with patch('business.SubscriptionSyncBusinessLogic.SMUSAdapter', return_value=mock_smus_adapter):
            with patch('business.SubscriptionSyncBusinessLogic.CollibraAdapter', return_value=mock_collibra_adapter):
                return SubscriptionSyncBusinessLogic(mock_logger)

    def test_sync_subscription_to_collibra_ignores_admin_role_requests(self, business_logic, mock_smus_adapter, mock_logger):
        """Test sync_subscription_to_collibra ignores requests from admin role"""
        with patch('business.SubscriptionSyncBusinessLogic.SMUS_COLLIBRA_INTEGRATION_ADMIN_ROLE_ARN', 'arn:aws:iam::123456789012:role/AdminRole'):
            mock_smus_adapter.get_user_profile.return_value = {
                'type': 'IAM',
                'details': {
                    'iam': {
                        'arn': 'arn:aws:iam::123456789012:role/AdminRole'
                    }
                }
            }
            event = {
                'requesterId': 'admin-user',
                'status': 'PENDING',
                'subscribedPrincipals': [{'id': 'proj-1'}],
                'subscribedListings': [{'ownerProjectId': 'proj-2', 'item': {'assetListing': {}}}]
            }
            
            business_logic.sync_subscription_to_collibra(event)
            
            assert any('Subscription request created by SMUS Admin Role, thus ignoring.' in str(call) for call in mock_logger.info.call_args_list)

    def test_sync_subscription_to_collibra_rejects_non_pending_status(self, business_logic, mock_smus_adapter, mock_logger):
        """Test sync_subscription_to_collibra rejects non-PENDING status"""
        mock_smus_adapter.get_user_profile.return_value = {'type': 'SSO'}
        event = {
            'requesterId': 'user-1',
            'status': 'APPROVED',
            'subscribedPrincipals': [{'id': 'proj-1'}],
            'subscribedListings': [{'ownerProjectId': 'proj-2', 'item': {'assetListing': {}}}]
        }
        
        business_logic.sync_subscription_to_collibra(event)
        
        assert any('Subscription request status is APPROVED. Expected PENDING' in str(call) for call in mock_logger.warn.call_args_list)

    def test_sync_subscription_to_collibra_rejects_multiple_principals(self, business_logic, mock_smus_adapter, mock_logger):
        """Test sync_subscription_to_collibra rejects multiple subscribed principals"""
        mock_smus_adapter.get_user_profile.return_value = {'type': 'SSO'}
        event = {
            'requesterId': 'user-1',
            'status': 'PENDING',
            'subscribedPrincipals': [{'id': 'proj-1'}, {'id': 'proj-2'}],
            'subscribedListings': [{'ownerProjectId': 'proj-2', 'item': {'assetListing': {}}}]
        }
        
        business_logic.sync_subscription_to_collibra(event)
        
        assert any('Expected only 1 subscribed principal.' in str(call) for call in mock_logger.warn.call_args_list)

    def test_sync_subscription_to_collibra_rejects_consumer_not_in_projects(self, business_logic, mock_smus_adapter, mock_logger):
        """Test sync_subscription_to_collibra rejects consumer not in allowed projects"""
        mock_smus_adapter.get_user_profile.return_value = {'type': 'SSO'}
        event = {
            'requesterId': 'user-1',
            'status': 'PENDING',
            'subscribedPrincipals': [{'id': 'proj-999'}],  # Not in allowed list
            'subscribedListings': [{'ownerProjectId': 'proj-2', 'item': {'assetListing': {}}}]
        }
        
        business_logic.sync_subscription_to_collibra(event)
        
        assert any('Subscriber must be in a project of which' in str(call) for call in mock_logger.warn.call_args_list)

    def test_sync_subscription_to_collibra_rejects_multiple_listings(self, business_logic, mock_smus_adapter, mock_logger):
        """Test sync_subscription_to_collibra rejects multiple subscribed listings"""
        mock_smus_adapter.get_user_profile.return_value = {'type': 'SSO'}
        event = {
            'requesterId': 'user-1',
            'status': 'PENDING',
            'subscribedPrincipals': [{'id': 'proj-1'}],
            'subscribedListings': [
                {'ownerProjectId': 'proj-2', 'item': {'assetListing': {}}},
                {'ownerProjectId': 'proj-2', 'item': {'assetListing': {}}}
            ]
        }
        
        business_logic.sync_subscription_to_collibra(event)
        
        assert any('No or multiple subscribed listings found. Expected 1' in str(call) for call in mock_logger.warn.call_args_list)

    def test_sync_subscription_to_collibra_rejects_owner_not_in_projects(self, business_logic, mock_smus_adapter, mock_logger):
        """Test sync_subscription_to_collibra rejects owner not in allowed projects"""
        mock_smus_adapter.get_user_profile.return_value = {'type': 'SSO'}
        event = {
            'requesterId': 'user-1',
            'status': 'PENDING',
            'subscribedPrincipals': [{'id': 'proj-1'}],
            'subscribedListings': [{'ownerProjectId': 'proj-999', 'item': {'assetListing': {}}}]  # Not in allowed list
        }
        
        business_logic.sync_subscription_to_collibra(event)
        
        assert any('Owner of the subscribed listing must be in a project of which' in str(call) for call in mock_logger.warn.call_args_list)

    def test_sync_subscription_to_collibra_rejects_non_asset_listing(self, business_logic, mock_smus_adapter, mock_logger):
        """Test sync_subscription_to_collibra rejects non-asset listings"""
        mock_smus_adapter.get_user_profile.return_value = {'type': 'SSO'}
        event = {
            'requesterId': 'user-1',
            'status': 'PENDING',
            'subscribedPrincipals': [{'id': 'proj-1'}],
            'subscribedListings': [{'ownerProjectId': 'proj-2', 'item': {}}]  # No assetListing
        }
        
        business_logic.sync_subscription_to_collibra(event)
        
        assert any('Subscribed listing is not an asset' in str(call) for call in mock_logger.warn.call_args_list)

    def test_sync_subscription_to_collibra_success(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test sync_subscription_to_collibra successfully creates workflow"""
        mock_smus_adapter.get_user_profile.return_value = {'type': 'SSO'}
        mock_smus_adapter.get_project.return_value = {'name': 'Consumer Project'}
        mock_smus_adapter.get_asset.return_value = {'name': 'customers_table'}
        mock_collibra_adapter.get_table_by_name.return_value = {'id': 'collibra-table-1'}
        mock_collibra_adapter.start_subscription_request_creation_workflow.return_value = {'workflowId': 'wf-1'}
        
        event = {
            'requesterId': 'user-1',
            'status': 'PENDING',
            'subscribedPrincipals': [{'id': 'proj-1'}],
            'subscribedListings': [{'ownerProjectId': 'proj-2', 'item': {'assetListing': {'entityId': 'asset-1'}}}]
        }
        
        business_logic.sync_subscription_to_collibra(event)
        
        mock_collibra_adapter.start_subscription_request_creation_workflow.assert_called_once()
        assert any('Successfully started subscription request workflow in Collibra' in str(call) for call in mock_logger.info.call_args_list)

    def test_sync_subscription_to_collibra_handles_exceptions(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test sync_subscription_to_collibra handles exceptions gracefully"""
        mock_smus_adapter.get_user_profile.return_value = {'type': 'SSO'}
        mock_smus_adapter.get_project.side_effect = Exception("Project not found")
        
        event = {
            'requesterId': 'user-1',
            'status': 'PENDING',
            'subscribedPrincipals': [{'id': 'proj-1'}],
            'subscribedListings': [{'ownerProjectId': 'proj-2', 'item': {'assetListing': {'entityId': 'asset-1'}}}]
        }
        
        business_logic.sync_subscription_to_collibra(event)
        
        mock_logger.error.assert_called_once()

    def test_start_subscription_request_sync_to_smus_processes_approved_requests(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test start_subscription_request_sync_to_smus processes approved requests"""
        mock_collibra_adapter.get_subscription_requests_by_status.return_value = [
            {
                'id': 'req-1',
                'stringAttributes': [
                    {'type': {'name': 'AWS Consumer Project Id'}, 'stringValue': 'proj-1'},
                    {'type': {'name': 'AWS Producer Project Id'}, 'stringValue': 'proj-2'}
                ],
                'outgoingRelations': [{'target': {'displayName': 'customers_table', 'id': 'collibra-table-1'}}]
            }
        ]
        mock_smus_adapter.search_all_listings.return_value = [
            {'assetListing': {'listingId': 'listing-1', 'name': 'customers_table'}}
        ]
        mock_smus_adapter.search_subscription_requests.return_value = []
        mock_smus_adapter.create_subscription_request.return_value = {'id': 'sub-req-1'}
        mock_smus_adapter.search_approved_subscription_for_subscription_request_id.return_value = [{'id': 'sub-1'}]
        
        with patch('business.CollibraSMUSListingMatcher.CollibraSMUSListingMatcher.match', return_value=True):
            business_logic.start_subscription_request_sync_to_smus()
        
        mock_smus_adapter.create_subscription_request.assert_called_once()
        mock_collibra_adapter.update_subscription_request_status.assert_called_once()

    def test_start_subscription_request_sync_to_smus_handles_empty_approved_requests(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test start_subscription_request_sync_to_smus handles empty approved requests"""
        mock_collibra_adapter.get_subscription_requests_by_status.return_value = []
        
        business_logic.start_subscription_request_sync_to_smus()
        
        assert any('Found 0 approved requests' in str(call) for call in mock_logger.info.call_args_list)

    def test_start_subscription_request_sync_to_smus_skips_existing_subscription(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test start_subscription_request_sync_to_smus skips when subscription already exists"""
        mock_collibra_adapter.get_subscription_requests_by_status.return_value = [
            {
                'id': 'req-1',
                'stringAttributes': [
                    {'type': {'name': 'AWS Consumer Project Id'}, 'stringValue': 'proj-1'},
                    {'type': {'name': 'AWS Producer Project Id'}, 'stringValue': 'proj-2'}
                ],
                'outgoingRelations': [{'target': {'displayName': 'customers_table', 'id': 'collibra-table-1'}}]
            }
        ]
        mock_smus_adapter.search_all_listings.return_value = [
            {'assetListing': {'listingId': 'listing-1', 'name': 'customers_table'}}
        ]
        mock_smus_adapter.search_subscription_requests.return_value = [{'id': 'existing-req'}]
        mock_smus_adapter.search_approved_subscription_for_subscription_request_id.return_value = [{'id': 'sub-1'}]
        
        with patch('business.CollibraSMUSListingMatcher.CollibraSMUSListingMatcher.match', return_value=True):
            business_logic.start_subscription_request_sync_to_smus()
        
        assert any('Subscription request already exists' in str(call) for call in mock_logger.info.call_args_list)
        mock_smus_adapter.create_subscription_request.assert_not_called()

    def test_start_subscription_request_sync_to_smus_skips_invalid_consumer_project(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test start_subscription_request_sync_to_smus skips requests with invalid consumer project"""
        mock_collibra_adapter.get_subscription_requests_by_status.return_value = [
            {
                'id': 'req-1',
                'stringAttributes': [
                    {'type': {'name': 'AWS Consumer Project Id'}, 'stringValue': 'proj-999'},  # Not in allowed list
                    {'type': {'name': 'AWS Producer Project Id'}, 'stringValue': 'proj-2'}
                ],
                'outgoingRelations': [{'target': {'displayName': 'table'}}]
            }
        ]
        
        business_logic.start_subscription_request_sync_to_smus()
        
        assert any('Subscriber must be in a project of which' in str(call) for call in mock_logger.warn.call_args_list)

    def test_start_subscription_request_sync_to_smus_skips_invalid_producer_project(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test start_subscription_request_sync_to_smus skips invalid producer project"""
        mock_collibra_adapter.get_subscription_requests_by_status.return_value = [
            {
                'id': 'req-1',
                'stringAttributes': [
                    {'type': {'name': 'AWS Consumer Project Id'}, 'stringValue': 'proj-1'},
                    {'type': {'name': 'AWS Producer Project Id'}, 'stringValue': 'proj-999'}  # Not in allowed list
                ],
                'outgoingRelations': [{'target': {'displayName': 'table'}}]
            }
        ]
        
        business_logic.start_subscription_request_sync_to_smus()
        
        assert any('Listing must be in a project of which' in str(call) for call in mock_logger.warn.call_args_list)

    def test_start_subscription_request_sync_to_smus_skips_when_no_listing_found(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test start_subscription_request_sync_to_smus skips when no matching listing found"""
        mock_collibra_adapter.get_subscription_requests_by_status.return_value = [
            {
                'id': 'req-1',
                'stringAttributes': [
                    {'type': {'name': 'AWS Consumer Project Id'}, 'stringValue': 'proj-1'},
                    {'type': {'name': 'AWS Producer Project Id'}, 'stringValue': 'proj-2'}
                ],
                'outgoingRelations': [{'target': {'displayName': 'nonexistent_table', 'id': 'collibra-table-1'}}]
            }
        ]
        mock_smus_adapter.search_all_listings.return_value = []
        
        business_logic.start_subscription_request_sync_to_smus()
        
        assert any('No listing found in SMUS for collibra asset nonexistent_table' in str(call) for call in mock_logger.info.call_args_list)

    def test_start_subscription_request_sync_to_smus_handles_request_processing_failure(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test start_subscription_request_sync_to_smus handles request processing failures"""
        mock_collibra_adapter.get_subscription_requests_by_status.return_value = [
            {
                'id': 'req-1',
                'stringAttributes': [
                    {'type': {'name': 'AWS Consumer Project Id'}, 'stringValue': 'proj-1'},
                    {'type': {'name': 'AWS Producer Project Id'}, 'stringValue': 'proj-2'}
                ],
                'outgoingRelations': [{'target': {'displayName': 'customers_table', 'id': 'collibra-table-1'}}]
            }
        ]
        mock_smus_adapter.search_all_listings.side_effect = Exception("Search failed")
        
        with patch('business.SubscriptionSyncBusinessLogic.COLLIBRA_SUBSCRIPTION_REQUEST_REJECTED_STATUS_ID', 'rejected-status'):
            business_logic.start_subscription_request_sync_to_smus()
        
        mock_logger.warn.assert_called()
        mock_collibra_adapter.update_subscription_request_status.assert_called_with('req-1', 'rejected-status')
