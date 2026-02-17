"""
Unit tests for lambda/utils/common_utils.py
"""
import pytest
from unittest.mock import MagicMock, patch
from time import time

from utils.common_utils import (
    get_collibra_synced_glossary_name,
    wait_until,
    extract_collibra_descriptions
)


@pytest.mark.unit
class TestGetCollibraSyncedGlossaryName:
    """Tests for get_collibra_synced_glossary_name function"""

    def test_returns_glossary_name_with_domain_id(self, mock_env_vars):
        """Test that glossary name includes the SMUS domain ID"""
        result = get_collibra_synced_glossary_name()
        
        assert result == 'CollibraSyncedGlossary-test-domain-id'
        assert 'CollibraSyncedGlossary-' in result
        assert mock_env_vars['SMUS_DOMAIN_ID'] in result


@pytest.mark.unit
class TestWaitUntil:
    """Tests for wait_until function"""

    def test_returns_immediately_when_condition_met(self, mock_logger):
        """Test that function returns immediately when condition is True"""
        condition = MagicMock(return_value=True)
        
        # Should not raise and should return quickly
        wait_until(1, 5, mock_logger, "Waiting...", condition, "arg1")
        
        condition.assert_called_once_with("arg1")
        mock_logger.info.assert_not_called()

    def test_waits_and_checks_condition_multiple_times(self, mock_logger):
        """Test that function checks condition multiple times before succeeding"""
        # Condition returns False twice, then True
        condition = MagicMock(side_effect=[False, False, True])
        
        with patch('utils.common_utils.sleep') as mock_sleep:
            wait_until(1, 10, mock_logger, "Waiting...", condition)
        
        assert condition.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(1)
        assert mock_logger.info.call_count == 2

    def test_raises_timeout_error_when_condition_never_met(self, mock_logger):
        """Test that TimeoutError is raised when condition is never met"""
        condition = MagicMock(return_value=False)
        
        with patch('utils.common_utils.sleep'):
            with patch('utils.common_utils.time', side_effect=[0, 1, 2, 3, 4, 5, 6]):
                with pytest.raises(TimeoutError) as exc_info:
                    wait_until(1, 5, mock_logger, "Waiting...", condition)
        
        assert "Condition not met within 5 seconds" in str(exc_info.value)

    def test_returns_when_method_is_none_and_timeout_reached(self, mock_logger):
        """Test that function returns without error when method_to_call is None"""
        with patch('utils.common_utils.sleep'):
            with patch('utils.common_utils.time', side_effect=[0, 1, 2, 3, 4, 5, 6]):
                # Should not raise
                wait_until(1, 5, mock_logger, "Waiting...", None)
        
        # Should have logged the wait message
        assert mock_logger.info.call_count > 0

    def test_passes_multiple_arguments_to_condition(self, mock_logger):
        """Test that multiple arguments are passed correctly to the condition"""
        condition = MagicMock(return_value=True)
        
        wait_until(1, 5, mock_logger, None, condition, "arg1", "arg2", "arg3")
        
        condition.assert_called_once_with("arg1", "arg2", "arg3")

    def test_does_not_log_when_wait_message_is_none(self, mock_logger):
        """Test that no logging occurs when wait_message is None"""
        condition = MagicMock(side_effect=[False, True])
        
        with patch('utils.common_utils.sleep'):
            wait_until(1, 5, mock_logger, None, condition)
        
        mock_logger.info.assert_not_called()

    def test_does_not_log_when_wait_message_is_empty(self, mock_logger):
        """Test that no logging occurs when wait_message is empty string"""
        condition = MagicMock(side_effect=[False, True])
        
        with patch('utils.common_utils.sleep'):
            wait_until(1, 5, mock_logger, "", condition)
        
        mock_logger.info.assert_not_called()


@pytest.mark.unit
class TestExtractCollibraDescriptions:
    """Tests for extract_collibra_descriptions function"""

    def test_extracts_single_description(self):
        """Test extracting a single description from asset"""
        asset = {
            'stringAttributes': [
                {'stringValue': 'Description 1'}
            ]
        }
        
        result = extract_collibra_descriptions(asset)
        
        assert result == ['Description 1']
        assert len(result) == 1

    def test_extracts_multiple_descriptions(self):
        """Test extracting multiple descriptions from asset"""
        asset = {
            'stringAttributes': [
                {'stringValue': 'Description 1'},
                {'stringValue': 'Description 2'},
                {'stringValue': 'Description 3'}
            ]
        }
        
        result = extract_collibra_descriptions(asset)
        
        assert result == ['Description 1', 'Description 2', 'Description 3']
        assert len(result) == 3

    def test_returns_empty_list_when_no_string_attributes(self):
        """Test returns empty list when stringAttributes key is missing"""
        asset = {'id': '123', 'name': 'Test Asset'}
        
        result = extract_collibra_descriptions(asset)
        
        assert result == []

    def test_returns_empty_list_when_string_attributes_is_none(self):
        """Test returns empty list when stringAttributes is None"""
        asset = {'stringAttributes': None}
        
        result = extract_collibra_descriptions(asset)
        
        assert result == []

    def test_returns_empty_list_when_string_attributes_is_empty(self):
        """Test returns empty list when stringAttributes is empty list"""
        asset = {'stringAttributes': []}
        
        result = extract_collibra_descriptions(asset)
        
        assert result == []

    def test_handles_empty_string_values(self):
        """Test handles empty string values in descriptions"""
        asset = {
            'stringAttributes': [
                {'stringValue': ''},
                {'stringValue': 'Valid description'},
                {'stringValue': ''}
            ]
        }
        
        result = extract_collibra_descriptions(asset)
        
        assert result == ['', 'Valid description', '']
        assert len(result) == 3

    def test_preserves_description_order(self):
        """Test that descriptions are returned in the same order"""
        asset = {
            'stringAttributes': [
                {'stringValue': 'First'},
                {'stringValue': 'Second'},
                {'stringValue': 'Third'}
            ]
        }
        
        result = extract_collibra_descriptions(asset)
        
        assert result[0] == 'First'
        assert result[1] == 'Second'
        assert result[2] == 'Third'
