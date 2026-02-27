"""
Unit tests for lambda/adapter/SMUSAdapter.py
"""
import pytest
from unittest.mock import MagicMock, patch

from adapter.SMUSAdapter import SMUSAdapter


@pytest.mark.unit
class TestSMUSAdapter:
    """Tests for SMUSAdapter class"""

    @pytest.fixture
    def mock_datazone_client(self):
        """Mock AWS DataZone client"""
        client = MagicMock()
        # Mock search_user_profiles for __find_admin_role_user_id
        client.search_user_profiles.return_value = {
            'items': [{
                'id': 'admin-user-id',
                'status': 'ACTIVATED',
                'details': {
                    'iam': {
                        'arn': 'arn:aws:iam::123456789012:role/TestRole'
                    }
                }
            }]
        }
        return client

    @pytest.fixture
    def adapter(self, mock_logger, mock_datazone_client):
        """Create SMUSAdapter instance with mocked dependencies"""
        with patch('adapter.SMUSAdapter.AWSClientFactory.create', return_value=mock_datazone_client):
            return SMUSAdapter(mock_logger)

    def test_init_finds_admin_role_user_id(self, mock_logger, mock_datazone_client):
        """Test initialization successfully creates adapter and finds admin user"""
        with patch('adapter.SMUSAdapter.AWSClientFactory.create', return_value=mock_datazone_client):
            adapter = SMUSAdapter(mock_logger)
            
            # Verify behavior: adapter was created and admin lookup was performed
            mock_datazone_client.search_user_profiles.assert_called()
            # Verify adapter can make calls (behavior test, not internal state)
            mock_datazone_client.get_project.return_value = {'id': 'test'}
            result = adapter.get_project('test')
            assert result['id'] == 'test'

    def test_get_project_success(self, adapter, mock_datazone_client):
        """Test get_project returns project data"""
        mock_datazone_client.get_project.return_value = {
            'id': 'proj-123',
            'name': 'MyProject'
        }
        
        result = adapter.get_project('proj-123')
        
        assert result['id'] == 'proj-123'
        assert result['name'] == 'MyProject'

    def test_create_or_get_glossary_returns_existing(self, adapter, mock_datazone_client, mock_logger):
        """Test create_or_get_glossary returns existing glossary"""
        mock_datazone_client.search.return_value = {
            'items': [{
                'glossaryItem': {
                    'id': 'glossary-123',
                    'name': 'CollibraSyncedGlossary-test-domain-id'
                }
            }]
        }
        
        result = adapter.create_or_get_glossary()
        
        assert result == 'glossary-123'
        mock_logger.info.assert_called()

    @patch('adapter.SMUSAdapter.wait_until')
    def test_create_or_get_glossary_creates_new(self, mock_wait, adapter, mock_datazone_client, mock_logger):
        """Test create_or_get_glossary creates new glossary when not found"""
        mock_datazone_client.search.return_value = {'items': []}
        mock_datazone_client.create_glossary.return_value = {
            'id': 'glossary-new'
        }
        
        result = adapter.create_or_get_glossary()
        
        assert result == 'glossary-new'
        mock_datazone_client.create_glossary.assert_called_once()
        mock_wait.assert_called_once()

    def test_search_glossary_term_by_name_found(self, adapter, mock_datazone_client):
        """Test search_glossary_term_by_name returns term when found"""
        mock_datazone_client.search.return_value = {
            'items': [{
                'glossaryTermItem': {
                    'id': 'term-123',
                    'name': 'Customer ID'
                }
            }]
        }
        
        result = adapter.search_glossary_term_by_name('glossary-123', 'Customer ID')
        
        assert result['id'] == 'term-123'
        assert result['name'] == 'Customer ID'

    def test_search_glossary_term_by_name_not_found(self, adapter, mock_datazone_client):
        """Test search_glossary_term_by_name returns None when not found"""
        mock_datazone_client.search.return_value = {'items': []}
        
        result = adapter.search_glossary_term_by_name('glossary-123', 'NonExistent')
        
        assert result is None

    def test_search_glossary_term_by_name_wrong_name(self, adapter, mock_datazone_client):
        """Test search_glossary_term_by_name returns None when name doesn't match"""
        mock_datazone_client.search.return_value = {
            'items': [{
                'glossaryTermItem': {
                    'id': 'term-123',
                    'name': 'Different Name'
                }
            }]
        }
        
        result = adapter.search_glossary_term_by_name('glossary-123', 'Customer ID')
        
        assert result is None

    def test_create_glossary_term_with_short_description(self, adapter, mock_datazone_client):
        """Test create_glossary_term with short description"""
        mock_datazone_client.create_glossary_term.return_value = {'id': 'term-123'}
        
        adapter.create_glossary_term('glossary-123', 'Term Name', ['Short desc'])
        
        call_args = mock_datazone_client.create_glossary_term.call_args
        assert call_args[1]['name'] == 'Term Name'
        assert call_args[1]['shortDescription'] == 'Short desc'

    def test_create_glossary_term_with_long_description(self, adapter, mock_datazone_client):
        """Test create_glossary_term with long description"""
        mock_datazone_client.create_glossary_term.return_value = {'id': 'term-123'}
        
        adapter.create_glossary_term('glossary-123', 'Term Name', ['Desc 1', 'Desc 2'])
        
        call_args = mock_datazone_client.create_glossary_term.call_args
        assert 'longDescription' in call_args[1]
        assert 'Desc 1\n\nDesc 2' in call_args[1]['longDescription']

    def test_create_glossary_term_with_no_description(self, adapter, mock_datazone_client):
        """Test create_glossary_term with no description"""
        mock_datazone_client.create_glossary_term.return_value = {'id': 'term-123'}
        
        adapter.create_glossary_term('glossary-123', 'Term Name', [])
        
        call_args = mock_datazone_client.create_glossary_term.call_args
        assert 'shortDescription' not in call_args[1]
        assert 'longDescription' not in call_args[1]

    def test_update_glossary_term_description_with_short_desc(self, adapter, mock_datazone_client):
        """Test update_glossary_term_description with short description"""
        mock_datazone_client.update_glossary_term.return_value = {'id': 'term-123'}
        
        adapter.update_glossary_term_description('term-123', ['Short desc'])
        
        call_args = mock_datazone_client.update_glossary_term.call_args
        assert call_args[1]['identifier'] == 'term-123'
        assert call_args[1]['shortDescription'] == 'Short desc'

    def test_search_all_assets_by_name_single_page(self, adapter, mock_datazone_client):
        """Test search_all_assets_by_name with single page of results"""
        mock_datazone_client.search.return_value = {
            'items': [{'assetItem': {'id': 'asset-1'}}]
        }
        
        result = adapter.search_all_assets_by_name('table1', 'proj-123')
        
        assert len(result) == 1
        assert result[0]['assetItem']['id'] == 'asset-1'

    def test_search_all_assets_by_name_multiple_pages(self, adapter, mock_datazone_client):
        """Test search_all_assets_by_name with pagination"""
        mock_datazone_client.search.side_effect = [
            {'items': [{'assetItem': {'id': 'asset-1'}}], 'nextToken': 'token1'},
            {'items': [{'assetItem': {'id': 'asset-2'}}]}
        ]
        
        result = adapter.search_all_assets_by_name('table1', 'proj-123')
        
        assert len(result) == 2
        assert result[0]['assetItem']['id'] == 'asset-1'
        assert result[1]['assetItem']['id'] == 'asset-2'

    def test_search_asset_by_name_success(self, adapter, mock_datazone_client):
        """Test search_asset_by_name returns search results"""
        mock_datazone_client.search.return_value = {
            'items': [{'assetItem': {'id': 'asset-1'}}]
        }
        
        result = adapter.search_asset_by_name('table1', 'proj-123')
        
        assert len(result['items']) == 1

    def test_search_asset_by_name_with_next_token(self, adapter, mock_datazone_client):
        """Test search_asset_by_name passes pagination token correctly"""
        mock_datazone_client.search.return_value = {
            'items': [{'assetItem': {'id': 'asset-1'}}]
        }
        
        result = adapter.search_asset_by_name('table1', 'proj-123', 'token123')
        
        # Verify behavior: returns results with pagination
        assert len(result['items']) == 1
        call_args = mock_datazone_client.search.call_args
        assert call_args[1]['nextToken'] == 'token123'

    def test_search_all_listings_single_page(self, adapter, mock_datazone_client):
        """Test search_all_listings with single page"""
        mock_datazone_client.search_listings.return_value = {
            'items': [{'assetListing': {'listingId': 'listing-1'}}]
        }
        
        result = adapter.search_all_listings('proj-123')
        
        assert len(result) == 1

    def test_search_all_listings_multiple_pages(self, adapter, mock_datazone_client):
        """Test search_all_listings with pagination"""
        mock_datazone_client.search_listings.side_effect = [
            {'items': [{'assetListing': {'listingId': 'listing-1'}}], 'nextToken': 'token1'},
            {'items': [{'assetListing': {'listingId': 'listing-2'}}]}
        ]
        
        result = adapter.search_all_listings('proj-123')
        
        assert len(result) == 2

    def test_search_listings_with_search_text(self, adapter, mock_datazone_client):
        """Test search_listings passes search text correctly"""
        mock_datazone_client.search_listings.return_value = {'items': []}
        
        adapter.search_listings('proj-123', 'search-term')
        
        # Verify behavior: search text is passed to API
        call_args = mock_datazone_client.search_listings.call_args
        assert call_args[1]['searchText'] == 'search-term'

    def test_list_all_terms_in_glossary_single_page(self, adapter, mock_datazone_client):
        """Test list_all_terms_in_glossary with single page"""
        mock_datazone_client.search.return_value = {
            'items': [{'glossaryTermItem': {'id': 'term-1'}}]
        }
        
        result = adapter.list_all_terms_in_glossary('glossary-123')
        
        assert len(result) == 1

    def test_list_all_terms_in_glossary_multiple_pages(self, adapter, mock_datazone_client):
        """Test list_all_terms_in_glossary with pagination"""
        mock_datazone_client.search.side_effect = [
            {'items': [{'glossaryTermItem': {'id': 'term-1'}}], 'nextToken': 'token1'},
            {'items': [{'glossaryTermItem': {'id': 'term-2'}}]}
        ]
        
        result = adapter.list_all_terms_in_glossary('glossary-123')
        
        assert len(result) == 2

    def test_list_all_users_in_project_single_page(self, adapter, mock_datazone_client):
        """Test list_all_users_in_project with single page"""
        mock_datazone_client.list_project_memberships.return_value = {
            'members': [{'memberDetails': {'user': {'userId': 'user-1'}}}]
        }
        
        result = adapter.list_all_users_in_project('proj-123')
        
        assert len(result) == 1

    def test_list_all_users_in_project_filters_non_sso_users(self, adapter, mock_datazone_client):
        """Test list_all_users_in_project filters out non-SSO users"""
        mock_datazone_client.list_project_memberships.return_value = {
            'members': [
                {'memberDetails': {'user': {'userId': 'user-1'}}},
                {'memberDetails': {'group': {'groupId': 'group-1'}}}
            ]
        }
        
        result = adapter.list_all_users_in_project('proj-123')
        
        assert len(result) == 1
        assert 'user' in result[0]['memberDetails']

    def test_get_user_profile_success(self, adapter, mock_datazone_client):
        """Test get_user_profile returns user profile"""
        mock_datazone_client.get_user_profile.return_value = {
            'id': 'user-123',
            'type': 'SSO'
        }
        
        result = adapter.get_user_profile('user-123')
        
        assert result['id'] == 'user-123'

    def test_get_asset_success(self, adapter, mock_datazone_client):
        """Test get_asset returns asset data"""
        mock_datazone_client.get_asset.return_value = {
            'id': 'asset-123',
            'name': 'MyAsset'
        }
        
        result = adapter.get_asset('asset-123')
        
        assert result['id'] == 'asset-123'

    def test_create_asset_revision_success(self, adapter, mock_datazone_client):
        """Test create_asset_revision creates revision"""
        mock_datazone_client.create_asset_revision.return_value = {
            'id': 'asset-123',
            'revision': '2'
        }
        
        result = adapter.create_asset_revision('AssetName', 'asset-123', [])
        
        assert result['revision'] == '2'

    def test_update_glossary_term_relations_success(self, adapter, mock_datazone_client):
        """Test update_glossary_term_relations updates relations"""
        mock_datazone_client.update_glossary_term.return_value = {
            'id': 'term-123'
        }
        
        result = adapter.update_glossary_term_relations('glossary-123', 'term-123', 'Term Name', ['rel-1'])
        
        assert result['id'] == 'term-123'

    def test_create_subscription_request_success(self, adapter, mock_datazone_client):
        """Test create_subscription_request creates request"""
        mock_datazone_client.create_subscription_request.return_value = {
            'id': 'sub-req-123'
        }
        
        result = adapter.create_subscription_request('listing-123', 'proj-456')
        
        assert result['id'] == 'sub-req-123'

    def test_search_subscription_requests_success(self, adapter, mock_datazone_client):
        """Test search_subscription_requests returns sorted requests"""
        mock_datazone_client.list_subscription_requests.return_value = {
            'items': [
                {'id': 'req-1', 'updatedAt': '2024-01-02'},
                {'id': 'req-2', 'updatedAt': '2024-01-01'}
            ]
        }
        
        result = adapter.search_subscription_requests('listing-123', 'proj-owner', 'proj-consumer')
        
        assert len(result) == 2
        assert result[0]['id'] == 'req-1'  # Most recent first

    def test_search_approved_subscription_for_subscription_request_id(self, adapter, mock_datazone_client):
        """Test search_approved_subscription_for_subscription_request_id returns subscriptions"""
        mock_datazone_client.list_subscriptions.return_value = {
            'items': [{'id': 'sub-1'}]
        }
        
        result = adapter.search_approved_subscription_for_subscription_request_id('req-123', 'proj-owner', 'proj-consumer')
        
        assert len(result) == 1

    def test_accept_subscription_request_success(self, adapter, mock_datazone_client):
        """Test accept_subscription_request accepts request"""
        mock_datazone_client.accept_subscription_request.return_value = {
            'id': 'req-123',
            'status': 'ACCEPTED'
        }
        
        result = adapter.accept_subscription_request('req-123')
        
        assert result['status'] == 'ACCEPTED'

    def test_list_all_projects_filters_inactive(self, adapter, mock_datazone_client):
        """Test list_all_projects filters out inactive projects"""
        mock_datazone_client.list_projects.return_value = {
            'items': [
                {'id': 'proj-1', 'projectStatus': 'ACTIVE'},
                {'id': 'proj-2', 'projectStatus': 'DELETED'}
            ]
        }
        
        result = adapter.list_all_projects()
        
        assert len(result) == 1
        assert result[0]['id'] == 'proj-1'

    def test_list_projects_with_pagination(self, adapter, mock_datazone_client):
        """Test list_projects passes pagination token correctly"""
        mock_datazone_client.list_projects.return_value = {'items': []}
        
        adapter.list_projects(50, 'token123')
        
        # Verify behavior: pagination token is passed
        call_args = mock_datazone_client.list_projects.call_args
        assert call_args[1]['nextToken'] == 'token123'
