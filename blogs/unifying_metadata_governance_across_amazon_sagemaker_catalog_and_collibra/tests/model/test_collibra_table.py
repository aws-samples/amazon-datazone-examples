"""
Unit tests for lambda/model/CollibraTable.py
"""
import pytest
from unittest.mock import MagicMock

from model.CollibraTable import CollibraBusinessTerm, CollibraColumn, CollibraTable


@pytest.mark.unit
class TestCollibraBusinessTerm:
    """Tests for CollibraBusinessTerm class"""

    def test_init_with_valid_data(self):
        """Test initialization with valid business term data"""
        business_term = {
            'displayName': 'Customer ID',
            'id': 'term-123'
        }
        smus_term_id = 'smus-id-456'
        
        term = CollibraBusinessTerm(business_term, smus_term_id)
        
        assert term.name == 'Customer ID'
        assert term.smus_term_id == 'smus-id-456'


@pytest.mark.unit
class TestCollibraColumn:
    """Tests for CollibraColumn class"""

    @pytest.fixture
    def mock_glossary_cache(self):
        """Provides a mock SMUSGlossaryCache"""
        cache = MagicMock()
        cache.is_term_present.return_value = True
        cache.get_smus_term_id.side_effect = lambda name: f"id-{name}"
        return cache

    def test_init_with_column_name_only(self, mock_glossary_cache):
        """Test initialization with column name only"""
        column = {'displayName': 'customer_id'}
        
        col = CollibraColumn(column, mock_glossary_cache)
        
        assert col.name == 'customer_id'
        assert col.business_terms == []
        assert col.description == ''

    def test_init_with_single_description(self, mock_glossary_cache):
        """Test initialization with single description"""
        column = {
            'displayName': 'customer_id',
            'stringAttributes': [{'stringValue': 'Customer identifier'}]
        }
        
        col = CollibraColumn(column, mock_glossary_cache)
        
        assert col.description == 'Customer identifier'

    def test_init_with_multiple_descriptions(self, mock_glossary_cache):
        """Test initialization with multiple descriptions"""
        column = {
            'displayName': 'customer_id',
            'stringAttributes': [
                {'stringValue': 'Customer identifier'},
                {'stringValue': 'Primary key'}
            ]
        }
        
        col = CollibraColumn(column, mock_glossary_cache)
        
        assert col.description == 'Customer identifier,Primary key'

    def test_init_truncates_long_description(self, mock_glossary_cache):
        """Test initialization truncates description to 4096 chars"""
        long_desc = 'A' * 5000
        column = {
            'displayName': 'customer_id',
            'stringAttributes': [{'stringValue': long_desc}]
        }
        
        col = CollibraColumn(column, mock_glossary_cache)
        
        assert len(col.description) == 4096
        assert col.description == long_desc[:4096]

    def test_init_with_business_terms(self, mock_glossary_cache):
        """Test initialization with business terms"""
        column = {
            'displayName': 'customer_id',
            'incomingRelations': [
                {
                    'source': {
                        'displayName': 'Customer ID',
                        'id': 'term-123'
                    }
                }
            ]
        }
        
        col = CollibraColumn(column, mock_glossary_cache)
        
        assert len(col.business_terms) == 1
        assert col.business_terms[0].name == 'Customer ID'
        assert col.business_terms[0].smus_term_id == 'id-Customer ID'

    def test_init_skips_business_terms_not_in_cache(self, mock_glossary_cache):
        """Test initialization skips business terms not in cache"""
        mock_glossary_cache.is_term_present.return_value = False
        column = {
            'displayName': 'customer_id',
            'incomingRelations': [
                {
                    'source': {
                        'displayName': 'Unknown Term',
                        'id': 'term-123'
                    }
                }
            ]
        }
        
        col = CollibraColumn(column, mock_glossary_cache)
        
        assert col.business_terms == []

    def test_get_business_term_ids_returns_list(self, mock_glossary_cache):
        """Test get_business_term_ids returns list of IDs"""
        column = {
            'displayName': 'customer_id',
            'incomingRelations': [
                {'source': {'displayName': 'Term1', 'id': 't1'}},
                {'source': {'displayName': 'Term2', 'id': 't2'}}
            ]
        }
        
        col = CollibraColumn(column, mock_glossary_cache)
        
        ids = col.get_business_term_ids()
        assert ids == ['id-Term1', 'id-Term2']

    def test_init_handles_missing_source_in_relations(self, mock_glossary_cache):
        """Test initialization handles missing source in relations"""
        column = {
            'displayName': 'customer_id',
            'incomingRelations': [
                {'target': {'displayName': 'Something'}}
            ]
        }
        
        col = CollibraColumn(column, mock_glossary_cache)
        
        assert col.business_terms == []


@pytest.mark.unit
class TestCollibraTable:
    """Tests for CollibraTable class"""

    @pytest.fixture
    def mock_glossary_cache(self):
        """Provides a mock SMUSGlossaryCache"""
        cache = MagicMock()
        cache.is_term_present.return_value = True
        cache.get_smus_term_id.side_effect = lambda name: f"id-{name}"
        return cache

    def test_init_with_minimal_data(self, mock_glossary_cache):
        """Test initialization with minimal table data"""
        table = {'displayName': 'customers'}
        business_terms_response = {}
        pii_columns_response = {}
        smus_asset_ids = ['asset-123']
        
        tbl = CollibraTable(table, business_terms_response, pii_columns_response, 
                           smus_asset_ids, mock_glossary_cache)
        
        assert tbl.name == 'customers'
        assert tbl.smus_asset_ids == ['asset-123']
        assert tbl.columns == {}
        assert tbl.description == ''
        assert tbl.business_terms == []
        assert tbl.pii_columns == []

    def test_init_with_description(self, mock_glossary_cache):
        """Test initialization with table description"""
        table = {
            'displayName': 'customers',
            'stringAttributes': [{'stringValue': 'Customer data table'}]
        }
        business_terms_response = {}
        pii_columns_response = {}
        smus_asset_ids = ['asset-123']
        
        tbl = CollibraTable(table, business_terms_response, pii_columns_response,
                           smus_asset_ids, mock_glossary_cache)
        
        assert tbl.description == 'Customer data table'

    def test_init_truncates_long_description(self, mock_glossary_cache):
        """Test initialization truncates description to 2048 chars"""
        long_desc = 'A' * 3000
        table = {
            'displayName': 'customers',
            'stringAttributes': [{'stringValue': long_desc}]
        }
        business_terms_response = {}
        pii_columns_response = {}
        smus_asset_ids = ['asset-123']
        
        tbl = CollibraTable(table, business_terms_response, pii_columns_response,
                           smus_asset_ids, mock_glossary_cache)
        
        assert len(tbl.description) == 2048
        assert tbl.description == long_desc[:2048]

    def test_init_with_columns(self, mock_glossary_cache):
        """Test initialization with columns"""
        table = {
            'displayName': 'customers',
            'incomingRelations': [
                {'source': {'displayName': 'customer_id'}},
                {'source': {'displayName': 'customer_name'}}
            ]
        }
        business_terms_response = {}
        pii_columns_response = {}
        smus_asset_ids = ['asset-123']
        
        tbl = CollibraTable(table, business_terms_response, pii_columns_response,
                           smus_asset_ids, mock_glossary_cache)
        
        assert len(tbl.columns) == 2
        assert 'customer_id' in tbl.columns
        assert 'customer_name' in tbl.columns

    def test_init_with_business_terms(self, mock_glossary_cache):
        """Test initialization with business terms"""
        table = {'displayName': 'customers'}
        business_terms_response = {
            'incomingRelations': [
                {'source': {'displayName': 'Customer Data', 'id': 'term-1'}}
            ]
        }
        pii_columns_response = {}
        smus_asset_ids = ['asset-123']
        
        tbl = CollibraTable(table, business_terms_response, pii_columns_response,
                           smus_asset_ids, mock_glossary_cache)
        
        assert len(tbl.business_terms) == 1
        assert tbl.business_terms[0].name == 'Customer Data'

    def test_get_business_term_ids_returns_list(self, mock_glossary_cache):
        """Test get_business_term_ids returns list of IDs"""
        table = {'displayName': 'customers'}
        business_terms_response = {
            'incomingRelations': [
                {'source': {'displayName': 'Term1', 'id': 't1'}},
                {'source': {'displayName': 'Term2', 'id': 't2'}}
            ]
        }
        pii_columns_response = {}
        smus_asset_ids = ['asset-123']
        
        tbl = CollibraTable(table, business_terms_response, pii_columns_response,
                           smus_asset_ids, mock_glossary_cache)
        
        ids = tbl.get_business_term_ids()
        assert ids == ['id-Term1', 'id-Term2']

    def test_init_with_pii_columns(self, mock_glossary_cache):
        """Test initialization with PII columns"""
        table = {'displayName': 'customers'}
        business_terms_response = {}
        pii_columns_response = {
            'incomingRelations': [
                {
                    'source': {
                        'displayName': 'ssn',
                        'incomingRelations': [
                            {
                                'source': {
                                    'incomingRelations': [
                                        {'source': {'displayName': 'PII'}}
                                    ]
                                }
                            }
                        ]
                    }
                }
            ]
        }
        smus_asset_ids = ['asset-123']
        
        tbl = CollibraTable(table, business_terms_response, pii_columns_response,
                           smus_asset_ids, mock_glossary_cache)
        
        assert tbl.pii_columns == ['ssn']

    def test_init_skips_non_pii_columns(self, mock_glossary_cache):
        """Test initialization skips columns without PII classification"""
        table = {'displayName': 'customers'}
        business_terms_response = {}
        pii_columns_response = {
            'incomingRelations': [
                {
                    'source': {
                        'displayName': 'customer_id',
                        'incomingRelations': [
                            {
                                'source': {
                                    'incomingRelations': []
                                }
                            }
                        ]
                    }
                }
            ]
        }
        smus_asset_ids = ['asset-123']
        
        tbl = CollibraTable(table, business_terms_response, pii_columns_response,
                           smus_asset_ids, mock_glossary_cache)
        
        assert tbl.pii_columns == []

    def test_init_handles_missing_source_in_pii_relations(self, mock_glossary_cache):
        """Test initialization handles missing source in PII relations"""
        table = {'displayName': 'customers'}
        business_terms_response = {}
        pii_columns_response = {
            'incomingRelations': [
                {'target': {'displayName': 'something'}}
            ]
        }
        smus_asset_ids = ['asset-123']
        
        tbl = CollibraTable(table, business_terms_response, pii_columns_response,
                           smus_asset_ids, mock_glossary_cache)
        
        assert tbl.pii_columns == []

    def test_init_with_multiple_smus_asset_ids(self, mock_glossary_cache):
        """Test initialization with multiple SMUS asset IDs"""
        table = {'displayName': 'customers'}
        business_terms_response = {}
        pii_columns_response = {}
        smus_asset_ids = ['asset-123', 'asset-456', 'asset-789']
        
        tbl = CollibraTable(table, business_terms_response, pii_columns_response,
                           smus_asset_ids, mock_glossary_cache)
        
        assert tbl.smus_asset_ids == ['asset-123', 'asset-456', 'asset-789']
