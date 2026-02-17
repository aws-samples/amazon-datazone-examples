"""
Unit tests for lambda/handler/business_metadata_sync_workflow/asset_metadata_sync_handler.py
"""
import pytest
from unittest.mock import MagicMock, patch


@pytest.mark.unit
class TestAssetMetadataSyncHandler:
    """Tests for asset_metadata_sync_handler"""

    @patch('handler.business_metadata_sync_workflow.asset_metadata_sync_handler.AssetMetadataSyncBusinessLogic')
    def test_handle_request_syncs_assets_with_last_seen_id(self, mock_business_logic_class):
        """Test handle_request syncs assets with last_seen_asset_id"""
        from handler.business_metadata_sync_workflow.asset_metadata_sync_handler import handle_request
        
        mock_business_logic = MagicMock()
        mock_business_logic.sync.return_value = "next-asset-id-123"
        mock_business_logic_class.return_value = mock_business_logic
        
        event = {"last_seen_asset_id": "asset-id-456"}
        context = MagicMock()
        
        result = handle_request(event, context)
        
        assert result["last_seen_asset_id"] == "next-asset-id-123"
        mock_business_logic.sync.assert_called_once_with("asset-id-456")

    @patch('handler.business_metadata_sync_workflow.asset_metadata_sync_handler.AssetMetadataSyncBusinessLogic')
    def test_handle_request_syncs_assets_without_last_seen_id(self, mock_business_logic_class):
        """Test handle_request syncs assets without last_seen_asset_id"""
        from handler.business_metadata_sync_workflow.asset_metadata_sync_handler import handle_request
        
        mock_business_logic = MagicMock()
        mock_business_logic.sync.return_value = "first-asset-id-789"
        mock_business_logic_class.return_value = mock_business_logic
        
        event = {}
        context = MagicMock()
        
        result = handle_request(event, context)
        
        assert result["last_seen_asset_id"] == "first-asset-id-789"
        mock_business_logic.sync.assert_called_once_with(None)

    @patch('handler.business_metadata_sync_workflow.asset_metadata_sync_handler.AssetMetadataSyncBusinessLogic')
    def test_handle_request_returns_none_when_sync_complete(self, mock_business_logic_class):
        """Test handle_request returns None when sync is complete"""
        from handler.business_metadata_sync_workflow.asset_metadata_sync_handler import handle_request
        
        mock_business_logic = MagicMock()
        mock_business_logic.sync.return_value = None
        mock_business_logic_class.return_value = mock_business_logic
        
        event = {"last_seen_asset_id": "last-asset-id"}
        context = MagicMock()
        
        result = handle_request(event, context)
        
        assert result["last_seen_asset_id"] is None

    @patch('handler.business_metadata_sync_workflow.asset_metadata_sync_handler.AssetMetadataSyncBusinessLogic')
    def test_handle_request_preserves_other_event_fields(self, mock_business_logic_class):
        """Test handle_request preserves other fields in event"""
        from handler.business_metadata_sync_workflow.asset_metadata_sync_handler import handle_request
        
        mock_business_logic = MagicMock()
        mock_business_logic.sync.return_value = "new-id"
        mock_business_logic_class.return_value = mock_business_logic
        
        event = {
            "last_seen_asset_id": "old-id",
            "other_field": "value",
            "another_field": 123
        }
        context = MagicMock()
        
        result = handle_request(event, context)
        
        assert result["last_seen_asset_id"] == "new-id"
        assert result["other_field"] == "value"
        assert result["another_field"] == 123
