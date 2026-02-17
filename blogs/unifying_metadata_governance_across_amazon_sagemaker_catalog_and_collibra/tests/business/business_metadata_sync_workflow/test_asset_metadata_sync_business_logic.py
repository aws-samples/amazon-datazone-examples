"""
Unit tests for lambda/business/business_metadata_sync_workflow/AssetMetadataSyncBusinessLogic.py
"""
import pytest
import json
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timedelta

from business.business_metadata_sync_workflow.AssetMetadataSyncBusinessLogic import AssetMetadataSyncBusinessLogic
from model.CollibraTable import CollibraTable, CollibraColumn


@pytest.mark.unit
class TestAssetMetadataSyncBusinessLogic:
    """Tests for AssetMetadataSyncBusinessLogic class"""

    @pytest.fixture
    def mock_smus_adapter(self):
        """Mock SMUS adapter"""
        adapter = MagicMock()
        adapter.list_all_projects.return_value = [{'id': 'proj-1'}, {'id': 'proj-2'}]
        return adapter

    @pytest.fixture
    def mock_collibra_adapter(self):
        """Mock Collibra adapter"""
        return MagicMock()

    @pytest.fixture
    def mock_glossary_cache(self):
        """Mock glossary cache"""
        return MagicMock()

    @pytest.fixture
    def business_logic(self, mock_logger, mock_smus_adapter, mock_collibra_adapter, mock_glossary_cache):
        """Create AssetMetadataSyncBusinessLogic instance with mocked dependencies"""
        with patch('business.business_metadata_sync_workflow.AssetMetadataSyncBusinessLogic.SMUSAdapter', return_value=mock_smus_adapter):
            with patch('business.business_metadata_sync_workflow.AssetMetadataSyncBusinessLogic.CollibraAdapter', return_value=mock_collibra_adapter):
                with patch('business.business_metadata_sync_workflow.AssetMetadataSyncBusinessLogic.SMUSGlossaryCache', return_value=mock_glossary_cache):
                    return AssetMetadataSyncBusinessLogic(mock_logger)

    def test_sync_filters_system_tables(self, business_logic, mock_collibra_adapter):
        """Test sync filters out information_schema tables"""
        mock_collibra_adapter.get_tables.return_value = [
            {'id': 'table-1', 'displayName': 'customers', 'fullName': 'db>customers'},
            {'id': 'table-2', 'displayName': 'sys_table', 'fullName': 'information_schema>sys_table'}
        ]
        
        result = business_logic.sync(None)
        
        # Should return last seen ID but only process non-system table
        assert result == 'table-2'

    def test_sync_handles_no_matching_assets(self, business_logic, mock_collibra_adapter, mock_smus_adapter, mock_logger):
        """Test sync handles tables with no matching SMUS assets"""
        mock_collibra_adapter.get_tables.return_value = [
            {'id': 'table-1', 'displayName': 'customers', 'fullName': 'db>customers'}
        ]
        mock_smus_adapter.search_all_assets_by_name.return_value = []
        
        result = business_logic.sync(None)
        
        assert any('No matching asset found in SMUS' in str(call) for call in mock_logger.info.call_args_list)

    def test_sync_handles_exceptions(self, business_logic, mock_collibra_adapter, mock_smus_adapter, mock_logger):
        """Test sync handles exceptions gracefully"""
        mock_collibra_adapter.get_tables.return_value = [
            {'id': 'table-1', 'displayName': 'customers', 'fullName': 'db>customers'}
        ]
        mock_smus_adapter.search_all_assets_by_name.side_effect = Exception("Search failed")
        
        result = business_logic.sync(None)
        
        mock_logger.error.assert_called()

    def test_sync_stops_when_no_more_tables(self, business_logic, mock_collibra_adapter):
        """Test sync stops when no more tables to process"""
        mock_collibra_adapter.get_tables.return_value = []
        
        result = business_logic.sync('last-id')
        
        assert result is None

    def test_sync_returns_last_seen_id(self, business_logic, mock_collibra_adapter):
        """Test sync returns last seen asset ID"""
        mock_collibra_adapter.get_tables.return_value = [
            {'id': 'table-1', 'displayName': 'customers', 'fullName': 'information_schema>customers'}
        ]
        
        result = business_logic.sync(None)
        
        assert result == 'table-1'

    def test_update_asset_metadata_with_description(self, business_logic, mock_smus_adapter):
        """Test update_asset_metadata includes description"""
        mock_smus_adapter.get_asset.return_value = {
            'id': 'asset-1',
            'formsOutput': [{
                'formName': 'GlueTableForm',
                'typeName': 'type1',
                'typeRevision': '1',
                'content': json.dumps({'columns': [{'columnName': 'id'}]})
            }]
        }
        
        mock_table = MagicMock(spec=CollibraTable)
        mock_table.smus_asset_ids = ['asset-1']
        mock_table.name = 'customers'
        mock_table.description = 'Customer data table'
        mock_table.pii_columns = []
        mock_table.columns = {}
        mock_table.get_business_term_ids.return_value = []
        
        business_logic.update_asset_metadata(mock_table)
        
        call_args = mock_smus_adapter.create_asset_revision.call_args
        assert 'description' in call_args.kwargs
        assert call_args.kwargs['description'] == 'Customer data table'

    def test_update_asset_metadata_with_glossary_terms(self, business_logic, mock_smus_adapter):
        """Test update_asset_metadata includes glossary terms"""
        mock_smus_adapter.get_asset.return_value = {
            'id': 'asset-1',
            'glossaryTerms': ['term-1'],
            'formsOutput': [{
                'formName': 'GlueTableForm',
                'typeName': 'type1',
                'typeRevision': '1',
                'content': json.dumps({'columns': [{'columnName': 'id'}]})
            }]
        }
        
        mock_table = MagicMock(spec=CollibraTable)
        mock_table.smus_asset_ids = ['asset-1']
        mock_table.name = 'customers'
        mock_table.description = None
        mock_table.pii_columns = []
        mock_table.columns = {}
        mock_table.get_business_term_ids.return_value = ['term-2']
        
        business_logic.update_asset_metadata(mock_table)
        
        call_args = mock_smus_adapter.create_asset_revision.call_args
        assert 'glossaryTerms' in call_args.kwargs
        assert set(call_args.kwargs['glossaryTerms']) == {'term-1', 'term-2'}

    def test_replace_data_category_from_readme_with_existing(self):
        """Test replace_data_category_from_readme replaces existing section"""
        existing = "Some text\n\n### Columns with Data Category - Personal Identifiable Information\n* old_column"
        new_section = "### Columns with Data Category - Personal Identifiable Information\n* new_column"
        
        result = AssetMetadataSyncBusinessLogic.replace_data_category_from_readme(existing, new_section)
        
        assert "Some text" in result
        assert "new_column" in result
        assert "old_column" not in result

    def test_replace_data_category_from_readme_with_none(self):
        """Test replace_data_category_from_readme handles None existing readme"""
        new_section = "### Columns with Data Category - Personal Identifiable Information\n* column1"
        
        result = AssetMetadataSyncBusinessLogic.replace_data_category_from_readme(None, new_section)
        
        assert result == new_section

    def test_update_asset_metadata_creates_column_business_metadata_form(self, business_logic, mock_smus_adapter):
        """Test update_asset_metadata creates ColumnBusinessMetadataForm if missing"""
        mock_smus_adapter.get_asset.return_value = {
            'id': 'asset-1',
            'formsOutput': [{
                'formName': 'GlueTableForm',
                'typeName': 'type1',
                'typeRevision': '1',
                'content': json.dumps({'columns': [{'columnName': 'id'}, {'columnName': 'name'}]})
            }]
        }
        
        mock_column = MagicMock(spec=CollibraColumn)
        mock_column.description = 'ID column'
        mock_column.get_business_term_ids.return_value = ['term-1']
        
        mock_table = MagicMock(spec=CollibraTable)
        mock_table.smus_asset_ids = ['asset-1']
        mock_table.name = 'customers'
        mock_table.description = None
        mock_table.pii_columns = []
        mock_table.columns = {'id': mock_column}
        mock_table.get_business_term_ids.return_value = []
        
        business_logic.update_asset_metadata(mock_table)
        
        call_args = mock_smus_adapter.create_asset_revision.call_args
        forms_input = call_args.args[2]
        form_names = [f['formName'] for f in forms_input]
        assert 'ColumnBusinessMetadataForm' in form_names

    def test_update_asset_metadata_updates_existing_column_business_metadata_form(self, business_logic, mock_smus_adapter):
        """Test update_asset_metadata updates existing ColumnBusinessMetadataForm"""
        mock_smus_adapter.get_asset.return_value = {
            'id': 'asset-1',
            'formsOutput': [{
                'formName': 'ColumnBusinessMetadataForm',
                'typeName': 'type1',
                'typeRevision': '1',
                'content': json.dumps({
                    'columnsBusinessMetadata': [
                        {'columnIdentifier': 'id'}
                    ]
                })
            }]
        }
        
        mock_column = MagicMock(spec=CollibraColumn)
        mock_column.description = 'ID column'
        mock_column.get_business_term_ids.return_value = ['term-1']
        
        mock_table = MagicMock(spec=CollibraTable)
        mock_table.smus_asset_ids = ['asset-1']
        mock_table.name = 'customers'
        mock_table.description = None
        mock_table.pii_columns = []
        mock_table.columns = {'id': mock_column}
        mock_table.get_business_term_ids.return_value = []
        
        business_logic.update_asset_metadata(mock_table)
        
        call_args = mock_smus_adapter.create_asset_revision.call_args
        forms_input = call_args.args[2]
        form_content = json.loads(forms_input[0]['content'])
        assert form_content['columnsBusinessMetadata'][0]['glossaryTerms'] == ['term-1']
        assert form_content['columnsBusinessMetadata'][0]['description'] == 'ID column'

    def test_update_asset_metadata_handles_redshift_table_form(self, business_logic, mock_smus_adapter):
        """Test update_asset_metadata handles RedshiftTableForm"""
        mock_smus_adapter.get_asset.return_value = {
            'id': 'asset-1',
            'formsOutput': [{
                'formName': 'RedshiftTableForm',
                'typeName': 'type1',
                'typeRevision': '1',
                'content': json.dumps({'columns': [{'columnName': 'id'}]})
            }]
        }
        
        mock_table = MagicMock(spec=CollibraTable)
        mock_table.smus_asset_ids = ['asset-1']
        mock_table.name = 'orders'
        mock_table.description = None
        mock_table.pii_columns = []
        mock_table.columns = {}
        mock_table.get_business_term_ids.return_value = []
        
        business_logic.update_asset_metadata(mock_table)
        
        call_args = mock_smus_adapter.create_asset_revision.call_args
        forms_input = call_args.args[2]
        form_names = [f['formName'] for f in forms_input]
        assert 'ColumnBusinessMetadataForm' in form_names

    def test_sync_searches_all_projects(self, business_logic, mock_collibra_adapter, mock_smus_adapter):
        """Test sync searches assets across all projects"""
        mock_collibra_adapter.get_tables.return_value = [
            {'id': 'table-1', 'displayName': 'customers', 'fullName': 'db>customers'}
        ]
        mock_smus_adapter.search_all_assets_by_name.return_value = []
        
        business_logic.sync(None)
        
        # Should search in both projects
        assert mock_smus_adapter.search_all_assets_by_name.call_count == 2
        calls = mock_smus_adapter.search_all_assets_by_name.call_args_list
        assert calls[0][0] == ('customers', 'proj-1')
        assert calls[1][0] == ('customers', 'proj-2')

    def test_update_asset_metadata_without_optional_fields(self, business_logic, mock_smus_adapter):
        """Test update_asset_metadata works without description and glossary terms"""
        mock_smus_adapter.get_asset.return_value = {
            'id': 'asset-1',
            'formsOutput': [{
                'formName': 'GlueTableForm',
                'typeName': 'type1',
                'typeRevision': '1',
                'content': json.dumps({'columns': [{'columnName': 'id'}]})
            }]
        }
        
        mock_table = MagicMock(spec=CollibraTable)
        mock_table.smus_asset_ids = ['asset-1']
        mock_table.name = 'customers'
        mock_table.description = None
        mock_table.pii_columns = []
        mock_table.columns = {}
        mock_table.get_business_term_ids.return_value = []
        
        business_logic.update_asset_metadata(mock_table)
        
        call_args = mock_smus_adapter.create_asset_revision.call_args
        assert 'description' not in call_args.kwargs
        assert 'glossaryTerms' not in call_args.kwargs

    def test_sync_processes_matching_assets(self, business_logic, mock_collibra_adapter, mock_smus_adapter, mock_logger):
        """Test sync processes tables with matching SMUS assets"""
        # First call returns tables, second call returns empty to exit loop
        mock_collibra_adapter.get_tables.side_effect = [
            [{'id': 'table-1', 'displayName': 'customers', 'fullName': 'db>customers'}],
            []
        ]
        # Return asset for first project, empty for second project
        mock_smus_adapter.search_all_assets_by_name.side_effect = [
            [{'assetItem': {'identifier': 'asset-1'}}],
            []
        ]
        mock_smus_adapter.get_asset.side_effect = [
            {'id': 'asset-1', 'name': 'customers', 'typeIdentifier': 'datazone:GlueTable'},
            {
                'id': 'asset-1',
                'name': 'customers',
                'formsOutput': [{
                    'formName': 'GlueTableForm',
                    'typeName': 'type1',
                    'typeRevision': '1',
                    'content': json.dumps({'columns': [{'columnName': 'id'}]})
                }]
            }
        ]
        mock_collibra_adapter.get_table.return_value = {'id': 'table-1', 'displayName': 'customers'}
        mock_collibra_adapter.get_table_business_terms.return_value = []
        mock_collibra_adapter.get_pii_columns.return_value = []
        
        with patch('business.CollibraSMUSAssetMatcher.CollibraSMUSAssetMatcher.match', return_value=True):
            with patch('model.CollibraTable.CollibraTable') as mock_table_class:
                mock_table = MagicMock(spec=CollibraTable)
                mock_table.smus_asset_ids = ['asset-1']
                mock_table.name = 'customers'
                mock_table.description = None
                mock_table.pii_columns = []
                mock_table.columns = {}
                mock_table.get_business_term_ids.return_value = []
                mock_table_class.return_value = mock_table
                
                business_logic.sync(None)
                
                assert any('Found 1 assets for collibra table customers in SMUS' in str(c) for c in mock_logger.info.call_args_list)
                assert any('Successfully updated asset with name customers in SMUS' in str(c) for c in mock_logger.info.call_args_list)

    def test_sync_calls_collibra_apis_for_table_data(self, business_logic, mock_collibra_adapter, mock_smus_adapter):
        """Test sync calls Collibra APIs to fetch table data"""
        # First call returns tables, second call returns empty to exit loop
        mock_collibra_adapter.get_tables.side_effect = [
            [{'id': 'table-1', 'displayName': 'customers', 'fullName': 'db>customers'}],
            []
        ]
        # Return asset for first project, empty for second project
        mock_smus_adapter.search_all_assets_by_name.side_effect = [
            [{'assetItem': {'identifier': 'asset-1'}}],
            []
        ]
        mock_smus_adapter.get_asset.side_effect = [
            {'id': 'asset-1', 'name': 'customers'},
            {'id': 'asset-1', 'formsOutput': [{'formName': 'GlueTableForm', 'typeName': 't', 'typeRevision': '1', 'content': '{}'}]}
        ]
        mock_collibra_adapter.get_table.return_value = {'id': 'table-1'}
        mock_collibra_adapter.get_table_business_terms.return_value = []
        mock_collibra_adapter.get_pii_columns.return_value = []
        
        with patch('business.CollibraSMUSAssetMatcher.CollibraSMUSAssetMatcher.match', return_value=True):
            with patch('model.CollibraTable.CollibraTable') as mock_table_class:
                mock_table = MagicMock(spec=CollibraTable)
                mock_table.smus_asset_ids = ['asset-1']
                mock_table.name = 'customers'
                mock_table.description = None
                mock_table.pii_columns = []
                mock_table.columns = {}
                mock_table.get_business_term_ids.return_value = []
                mock_table_class.return_value = mock_table
                
                business_logic.sync(None)
                
                mock_collibra_adapter.get_table.assert_called_once_with('table-1')
                mock_collibra_adapter.get_table_business_terms.assert_called_once_with('table-1')
                mock_collibra_adapter.get_pii_columns.assert_called_once_with('table-1')

    def test_update_asset_metadata_with_pii_columns_updates_readme(self, business_logic, mock_smus_adapter):
        """Test update_asset_metadata updates readme with PII columns"""
        mock_smus_adapter.get_asset.return_value = {
            'id': 'asset-1',
            'formsOutput': [{
                'formName': 'AssetCommonDetailsForm',
                'typeName': 'type1',
                'typeRevision': '1',
                'content': json.dumps({'readMe': 'Existing readme'})
            }, {
                'formName': 'GlueTableForm',
                'typeName': 'type2',
                'typeRevision': '1',
                'content': json.dumps({'columns': [{'columnName': 'id'}]})
            }]
        }
        
        mock_table = MagicMock(spec=CollibraTable)
        mock_table.smus_asset_ids = ['asset-1']
        mock_table.name = 'customers'
        mock_table.description = None
        mock_table.pii_columns = ['ssn', 'email']
        mock_table.columns = {}
        mock_table.get_business_term_ids.return_value = []
        
        business_logic.update_asset_metadata(mock_table)
        
        call_args = mock_smus_adapter.create_asset_revision.call_args
        forms_input = call_args.args[2]
        asset_form = [f for f in forms_input if f['formName'] == 'AssetCommonDetailsForm'][0]
        form_content = json.loads(asset_form['content'])
        assert '### Columns with Data Category' in form_content['readMe']
        assert 'ssn' in form_content['readMe']
        assert 'email' in form_content['readMe']

    def test_update_asset_metadata_skips_readme_update_when_no_pii_columns(self, business_logic, mock_smus_adapter):
        """Test update_asset_metadata skips readme update when no PII columns"""
        mock_smus_adapter.get_asset.return_value = {
            'id': 'asset-1',
            'formsOutput': [{
                'formName': 'AssetCommonDetailsForm',
                'typeName': 'type1',
                'typeRevision': '1',
                'content': json.dumps({'readme': 'Existing readme'})
            }, {
                'formName': 'GlueTableForm',
                'typeName': 'type2',
                'typeRevision': '1',
                'content': json.dumps({'columns': [{'columnName': 'id'}]})
            }]
        }
        
        mock_table = MagicMock(spec=CollibraTable)
        mock_table.smus_asset_ids = ['asset-1']
        mock_table.name = 'customers'
        mock_table.description = None
        mock_table.pii_columns = []  # No PII columns
        mock_table.columns = {}
        mock_table.get_business_term_ids.return_value = []
        
        business_logic.update_asset_metadata(mock_table)
        
        call_args = mock_smus_adapter.create_asset_revision.call_args
        forms_input = call_args.args[2]
        asset_form = [f for f in forms_input if f['formName'] == 'AssetCommonDetailsForm'][0]
        form_content = json.loads(asset_form['content'])
        # Readme should remain unchanged
        assert form_content['readme'] == 'Existing readme'

    def test_update_asset_metadata_handles_column_without_metadata(self, business_logic, mock_smus_adapter):
        """Test update_asset_metadata handles columns without description or terms"""
        mock_smus_adapter.get_asset.return_value = {
            'id': 'asset-1',
            'formsOutput': [{
                'formName': 'ColumnBusinessMetadataForm',
                'typeName': 'type1',
                'typeRevision': '1',
                'content': json.dumps({
                    'columnsBusinessMetadata': [
                        {'columnIdentifier': 'unknown_column'}
                    ]
                })
            }]
        }
        
        mock_table = MagicMock(spec=CollibraTable)
        mock_table.smus_asset_ids = ['asset-1']
        mock_table.name = 'customers'
        mock_table.description = None
        mock_table.pii_columns = []
        mock_table.columns = {}  # No column metadata
        mock_table.get_business_term_ids.return_value = []
        
        business_logic.update_asset_metadata(mock_table)
        
        # Should still process without errors
        mock_smus_adapter.create_asset_revision.assert_called_once()

    def test_update_asset_metadata_creates_form_when_no_columns_found(self, business_logic, mock_smus_adapter):
        """Test update_asset_metadata handles case when no columns found in forms"""
        mock_smus_adapter.get_asset.return_value = {
            'id': 'asset-1',
            'formsOutput': [{
                'formName': 'SomeOtherForm',
                'typeName': 'type1',
                'typeRevision': '1',
                'content': json.dumps({})
            }]
        }
        
        mock_table = MagicMock(spec=CollibraTable)
        mock_table.smus_asset_ids = ['asset-1']
        mock_table.name = 'customers'
        mock_table.description = None
        mock_table.pii_columns = []
        mock_table.columns = {}
        mock_table.get_business_term_ids.return_value = []
        
        business_logic.update_asset_metadata(mock_table)
        
        # Should not create ColumnBusinessMetadataForm when no columns found
        call_args = mock_smus_adapter.create_asset_revision.call_args
        forms_input = call_args.args[2]
        form_names = [f['formName'] for f in forms_input]
        assert 'ColumnBusinessMetadataForm' not in form_names
