"""
Unit tests for lambda/handler/start_subscription_request_sync_to_collibra_handler.py
"""
import pytest
from unittest.mock import MagicMock, patch


@pytest.mark.unit
class TestStartSubscriptionRequestSyncToCollibraHandler:
    """Tests for start_subscription_request_sync_to_collibra_handler"""

    @patch('handler.start_subscription_request_sync_to_collibra_handler.SubscriptionSyncBusinessLogic')
    def test_handle_request_syncs_subscription_to_collibra(self, mock_business_logic_class):
        """Test handle_request syncs subscription data to Collibra"""
        from handler.start_subscription_request_sync_to_collibra_handler import handle_request
        
        mock_business_logic = MagicMock()
        mock_business_logic_class.return_value = mock_business_logic
        
        event = {
            "detail": {
                "data": {
                    "subscriptionRequestId": "sub-123",
                    "status": "PENDING"
                }
            }
        }
        context = MagicMock()
        
        result = handle_request(event, context)
        
        assert result == event
        mock_business_logic.sync_subscription_to_collibra.assert_called_once_with(event["detail"]["data"])

    @patch('handler.start_subscription_request_sync_to_collibra_handler.SubscriptionSyncBusinessLogic')
    def test_handle_request_passes_subscription_data(self, mock_business_logic_class):
        """Test handle_request passes correct subscription data"""
        from handler.start_subscription_request_sync_to_collibra_handler import handle_request
        
        mock_business_logic = MagicMock()
        mock_business_logic_class.return_value = mock_business_logic
        
        subscription_data = {
            "subscriptionRequestId": "sub-456",
            "status": "PENDING",
            "projectId": "proj-123"
        }
        event = {"detail": {"data": subscription_data}}
        context = MagicMock()
        
        handle_request(event, context)
        
        mock_business_logic.sync_subscription_to_collibra.assert_called_once_with(subscription_data)

    @patch('handler.start_subscription_request_sync_to_collibra_handler.SubscriptionSyncBusinessLogic')
    def test_handle_request_returns_original_event(self, mock_business_logic_class):
        """Test handle_request returns the original event"""
        from handler.start_subscription_request_sync_to_collibra_handler import handle_request
        
        mock_business_logic = MagicMock()
        mock_business_logic_class.return_value = mock_business_logic
        
        event = {"detail": {"data": {"subscriptionRequestId": "sub-789"}}}
        context = MagicMock()
        
        result = handle_request(event, context)
        
        assert result is event
