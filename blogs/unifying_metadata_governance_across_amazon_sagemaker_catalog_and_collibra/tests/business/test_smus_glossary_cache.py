"""
Unit tests for lambda/business/SMUSGlossaryCache.py
"""
import pytest
from unittest.mock import MagicMock, patch

from business.SMUSGlossaryCache import SMUSGlossaryCache


@pytest.mark.unit
class TestSMUSGlossaryCache:
    """Tests for SMUSGlossaryCache class"""

    @pytest.fixture
    def mock_smus_adapter(self):
        """Mock SMUSAdapter"""
        adapter = MagicMock()
        adapter.create_or_get_glossary.return_value = 'glossary-123'
        adapter.list_all_terms_in_glossary.return_value = [
            {'glossaryTermItem': {'id': 'term-1', 'name': 'Customer ID'}},
            {'glossaryTermItem': {'id': 'term-2', 'name': 'Order Date'}}
        ]
        return adapter

    @patch('business.SMUSGlossaryCache.SMUSAdapter')
    def test_init_loads_glossary_cache(self, mock_adapter_class, mock_logger, mock_smus_adapter):
        """Test initialization loads glossary cache"""
        mock_adapter_class.return_value = mock_smus_adapter
        
        cache = SMUSGlossaryCache(mock_logger)
        
        mock_smus_adapter.create_or_get_glossary.assert_called_once()
        mock_smus_adapter.list_all_terms_in_glossary.assert_called_once()
        mock_logger.info.assert_called()

    @patch('business.SMUSGlossaryCache.SMUSAdapter')
    def test_is_term_present_returns_true_for_existing_term(self, mock_adapter_class, mock_logger, mock_smus_adapter):
        """Test is_term_present returns True for existing term"""
        mock_adapter_class.return_value = mock_smus_adapter
        cache = SMUSGlossaryCache(mock_logger)
        
        result = cache.is_term_present('Customer ID')
        
        assert result is True

    @patch('business.SMUSGlossaryCache.SMUSAdapter')
    def test_is_term_present_returns_false_for_missing_term(self, mock_adapter_class, mock_logger, mock_smus_adapter):
        """Test is_term_present returns False for missing term"""
        mock_adapter_class.return_value = mock_smus_adapter
        cache = SMUSGlossaryCache(mock_logger)
        
        result = cache.is_term_present('Nonexistent Term')
        
        assert result is False

    @patch('business.SMUSGlossaryCache.SMUSAdapter')
    def test_get_smus_term_id_returns_id_for_existing_term(self, mock_adapter_class, mock_logger, mock_smus_adapter):
        """Test get_smus_term_id returns ID for existing term"""
        mock_adapter_class.return_value = mock_smus_adapter
        cache = SMUSGlossaryCache(mock_logger)
        
        result = cache.get_smus_term_id('Customer ID')
        
        assert result == 'term-1'

    @patch('business.SMUSGlossaryCache.SMUSAdapter')
    def test_get_smus_term_id_returns_none_for_missing_term(self, mock_adapter_class, mock_logger, mock_smus_adapter):
        """Test get_smus_term_id returns None for missing term"""
        mock_adapter_class.return_value = mock_smus_adapter
        cache = SMUSGlossaryCache(mock_logger)
        
        result = cache.get_smus_term_id('Nonexistent Term')
        
        assert result is None

    @patch('business.SMUSGlossaryCache.SMUSAdapter')
    def test_init_with_empty_glossary(self, mock_adapter_class, mock_logger):
        """Test initialization with empty glossary"""
        mock_adapter = MagicMock()
        mock_adapter.create_or_get_glossary.return_value = 'glossary-empty'
        mock_adapter.list_all_terms_in_glossary.return_value = []
        mock_adapter_class.return_value = mock_adapter
        
        cache = SMUSGlossaryCache(mock_logger)
        
        assert cache.is_term_present('Any Term') is False
        assert cache.get_smus_term_id('Any Term') is None
