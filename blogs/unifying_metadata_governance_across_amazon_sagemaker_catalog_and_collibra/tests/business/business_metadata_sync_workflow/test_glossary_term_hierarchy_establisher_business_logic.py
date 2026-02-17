"""
Unit tests for lambda/business/business_metadata_sync_workflow/GlossaryTermHierarchyEstablisherBusinessLogic.py
"""
import pytest
from unittest.mock import MagicMock, patch

from business.business_metadata_sync_workflow.GlossaryTermHierarchyEstablisherBusinessLogic import GlossaryTermHierarchyEstablisherBusinessLogic


@pytest.mark.unit
class TestGlossaryTermHierarchyEstablisherBusinessLogic:
    """Tests for GlossaryTermHierarchyEstablisherBusinessLogic class"""

    @pytest.fixture
    def mock_smus_adapter(self):
        """Mock SMUS adapter"""
        adapter = MagicMock()
        adapter.create_or_get_glossary.return_value = 'glossary-123'
        return adapter

    @pytest.fixture
    def mock_collibra_adapter(self):
        """Mock Collibra adapter"""
        return MagicMock()

    @pytest.fixture
    def mock_glossary_cache(self):
        """Mock glossary cache"""
        cache = MagicMock()
        cache.is_term_present.return_value = True
        cache.get_smus_term_id.side_effect = lambda name: f"term-id-{name}"
        return cache

    @pytest.fixture
    def business_logic(self, mock_logger, mock_smus_adapter, mock_collibra_adapter, mock_glossary_cache):
        """Create GlossaryTermHierarchyEstablisherBusinessLogic instance with mocked dependencies"""
        with patch('business.business_metadata_sync_workflow.GlossaryTermHierarchyEstablisherBusinessLogic.SMUSAdapter', return_value=mock_smus_adapter):
            with patch('business.business_metadata_sync_workflow.GlossaryTermHierarchyEstablisherBusinessLogic.CollibraAdapter', return_value=mock_collibra_adapter):
                with patch('business.business_metadata_sync_workflow.GlossaryTermHierarchyEstablisherBusinessLogic.SMUSGlossaryCache', return_value=mock_glossary_cache):
                    return GlossaryTermHierarchyEstablisherBusinessLogic(mock_logger)

    def test_init_creates_glossary(self, mock_logger, mock_smus_adapter, mock_collibra_adapter, mock_glossary_cache):
        """Test initialization creates or gets glossary"""
        with patch('business.business_metadata_sync_workflow.GlossaryTermHierarchyEstablisherBusinessLogic.SMUSAdapter', return_value=mock_smus_adapter):
            with patch('business.business_metadata_sync_workflow.GlossaryTermHierarchyEstablisherBusinessLogic.CollibraAdapter', return_value=mock_collibra_adapter):
                with patch('business.business_metadata_sync_workflow.GlossaryTermHierarchyEstablisherBusinessLogic.SMUSGlossaryCache', return_value=mock_glossary_cache):
                    logic = GlossaryTermHierarchyEstablisherBusinessLogic(mock_logger)
                    
                    mock_smus_adapter.create_or_get_glossary.assert_called_once()

    def test_establish_updates_term_relations(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_glossary_cache, mock_logger):
        """Test establish updates glossary term relations"""
        mock_collibra_adapter.get_business_term_hierarchy.return_value = [
            {
                'displayName': 'Customer Data',
                'incomingRelations': [
                    {
                        'source': {
                            'displayName': 'Personal Information'
                        }
                    }
                ]
            }
        ]
        
        business_logic.establish()
        
        # Index creates bidirectional relationships, so both child and parent get updated
        assert mock_smus_adapter.update_glossary_term_relations.call_count == 2
        mock_logger.info.assert_any_call("Updated glossary term relations for 2 terms")

    def test_establish_handles_multiple_parent_terms(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_glossary_cache, mock_logger):
        """Test establish handles terms with multiple parents"""
        mock_collibra_adapter.get_business_term_hierarchy.return_value = [
            {
                'displayName': 'Customer ID',
                'incomingRelations': [
                    {'source': {'displayName': 'Identifier'}},
                    {'source': {'displayName': 'Customer Data'}}
                ]
            }
        ]
        
        business_logic.establish()
        
        # Child + 2 parents = 3 terms updated
        assert mock_smus_adapter.update_glossary_term_relations.call_count == 3

    def test_establish_skips_terms_without_relations(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_glossary_cache, mock_logger):
        """Test establish skips terms without incoming relations"""
        mock_collibra_adapter.get_business_term_hierarchy.return_value = [
            {
                'displayName': 'Root Term'
                # No incomingRelations
            }
        ]
        
        business_logic.establish()
        
        mock_smus_adapter.update_glossary_term_relations.assert_not_called()
        mock_logger.info.assert_any_call("Updated glossary term relations for 0 terms")

    def test_establish_handles_empty_hierarchy(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_logger):
        """Test establish handles empty hierarchy response"""
        mock_collibra_adapter.get_business_term_hierarchy.return_value = []
        
        business_logic.establish()
        
        mock_smus_adapter.update_glossary_term_relations.assert_not_called()
        mock_logger.info.assert_any_call("Fetched business term hierarchy from Collibra. Indexing 0 terms")

    def test_establish_handles_relations_without_source(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_glossary_cache):
        """Test establish handles relations without source"""
        mock_collibra_adapter.get_business_term_hierarchy.return_value = [
            {
                'displayName': 'Customer Data',
                'incomingRelations': [
                    {
                        'target': {'displayName': 'Something'}
                        # No 'source' key
                    }
                ]
            }
        ]
        
        business_logic.establish()
        
        # Should not crash, should skip this relation
        mock_smus_adapter.update_glossary_term_relations.assert_not_called()

    def test_establish_processes_multiple_terms(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_glossary_cache, mock_logger):
        """Test establish processes multiple terms"""
        mock_collibra_adapter.get_business_term_hierarchy.return_value = [
            {
                'displayName': 'Customer ID',
                'incomingRelations': [
                    {'source': {'displayName': 'Identifier'}}
                ]
            },
            {
                'displayName': 'Order Date',
                'incomingRelations': [
                    {'source': {'displayName': 'Date'}}
                ]
            }
        ]
        
        business_logic.establish()
        
        # 2 children + 2 parents = 4 terms updated
        assert mock_smus_adapter.update_glossary_term_relations.call_count == 4
        mock_logger.info.assert_any_call("Updated glossary term relations for 4 terms")

    def test_establish_logs_progress(self, business_logic, mock_smus_adapter, mock_collibra_adapter, mock_glossary_cache, mock_logger):
        """Test establish logs progress messages"""
        mock_collibra_adapter.get_business_term_hierarchy.return_value = [
            {
                'displayName': 'Customer Data',
                'incomingRelations': [
                    {'source': {'displayName': 'Personal Information'}}
                ]
            }
        ]
        
        business_logic.establish()
        
        mock_logger.info.assert_any_call("Fetching business term hierarchy from Collibra")
        mock_logger.info.assert_any_call("Fetched business term hierarchy from Collibra. Indexing 1 terms")
        mock_logger.info.assert_any_call("Initiating glossary term relation identification 2 terms")
        mock_logger.info.assert_any_call("Updating glossary term relations for Customer Data in glossary glossary-123")
