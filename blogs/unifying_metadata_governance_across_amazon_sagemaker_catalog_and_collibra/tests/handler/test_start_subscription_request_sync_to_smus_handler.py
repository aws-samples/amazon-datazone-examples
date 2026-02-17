"""
Unit tests for lambda/handler/start_subscription_request_sync_to_smus_handler.py
"""
import pytest
from unittest.mock import MagicMock, patch


@pytest.mark.unit
class TestStartSubscriptionRequestSyncToSmusHandler:
    """Tests for start_subscription_request_sync_to_smus_handler"""

    @patch('handler.start_subscription_request_sync_to_smus_handler.SubscriptionSyncBusinessLogic')
    def test_handle_request_syncs_subscriptions_to_smus(self, mock_business_logic_class):
        """Test handle_request syncs approved subscriptions to SMUS"""
        from handler.start_subscription_request_sync_to_smus_handler import handle_request
        
        mock_business_logic = MagicMock()
        mock_business_logic_class.return_value = mock_business_logic
        
        event = {"source": "aws.events"}
        context = MagicMock()
        
        result = handle_request(event, context)
        
        assert result == event
        mock_business_logic.start_subscription_request_sync_to_smus.assert_called_once()

    @patch('handler.start_subscription_request_sync_to_smus_handler.SubscriptionSyncBusinessLogic')
    def test_handle_request_calls_business_logic_without_parameters(self, mock_business_logic_class):
        """Test handle_request calls business logic without parameters"""
        from handler.start_subscription_request_sync_to_smus_handler import handle_request
        
        mock_business_logic = MagicMock()
        mock_business_logic_class.return_value = mock_business_logic
        
        event = {}
        context = MagicMock()
        
        handle_request(event, context)
        
        mock_business_logic.start_subscription_request_sync_to_smus.assert_called_once_with()

    @patch('handler.start_subscription_request_sync_to_smus_handler.SubscriptionSyncBusinessLogic')
    def test_handle_request_returns_original_event(self, mock_business_logic_class):
        """Test handle_request returns the original event"""
        from handler.start_subscription_request_sync_to_smus_handler import handle_request
        
        mock_business_logic = MagicMock()
        mock_business_logic_class.return_value = mock_business_logic
        
        event = {"scheduled": True}
        context = MagicMock()
        
        result = handle_request(event, context)
        
        assert result is event
