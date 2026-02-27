"""
Unit tests for lambda/adapter/CollibraAdapter.py
"""
import pytest
from unittest.mock import MagicMock, patch, Mock
import json

from adapter.CollibraAdapter import CollibraAdapter
from model.CollibraAssetType import CollibraAssetType
from utils.env_utils import COLLIBRA_SUBSCRIPTION_REQUEST_GRANTED_STATUS_ID


@pytest.mark.unit
class TestCollibraAdapter:
    """Tests for CollibraAdapter class"""

    @pytest.fixture
    def mock_secrets_client(self):
        """Mock AWS Secrets Manager client"""
        client = MagicMock()
        client.get_secret_value.return_value = {
            'SecretString': json.dumps({
                'url': 'test.collibra.com',
                'username': 'test_user',
                'password': 'test_pass'
            })
        }
        return client

    @pytest.fixture
    def adapter(self, mock_logger, mock_secrets_client):
        """Create CollibraAdapter instance with mocked dependencies"""
        with patch('adapter.CollibraAdapter.AWSClientFactory.create', return_value=mock_secrets_client):
            return CollibraAdapter(mock_logger)

    def test_init_creates_adapter_successfully(self, mock_logger, mock_secrets_client):
        """Test initialization creates adapter and can make API calls"""
        with patch('adapter.CollibraAdapter.AWSClientFactory.create', return_value=mock_secrets_client):
            with patch('adapter.CollibraAdapter.requests.post') as mock_post:
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {'data': {'assets': []}}
                mock_post.return_value = mock_response
                
                adapter = CollibraAdapter(mock_logger)
                
                # Verify adapter can make authenticated API calls
                result = adapter.get_business_term_metadata()
                assert result == []
                assert mock_post.called

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_business_term_metadata_success(self, mock_post, adapter, mock_logger):
        """Test get_business_term_metadata returns data on success"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'assets': [{'id': '1', 'name': 'Term1'}, {'id': '2', 'name': 'Term2'}]
            }
        }
        mock_post.return_value = mock_response
        
        result = adapter.get_business_term_metadata()
        
        assert len(result) == 2
        assert result[0]['id'] == '1'
        mock_logger.info.assert_called()

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_business_term_metadata_with_cursor(self, mock_post, adapter):
        """Test get_business_term_metadata with last_seen_id returns correct data"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'assets': [{'id': 'term-after-cursor', 'name': 'NextTerm'}]
            }
        }
        mock_post.return_value = mock_response
        
        result = adapter.get_business_term_metadata(last_seen_id='cursor-123')
        
        # Verify behavior: returns data after cursor
        assert len(result) == 1
        assert result[0]['id'] == 'term-after-cursor'

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_business_term_metadata_failure(self, mock_post, adapter):
        """Test get_business_term_metadata raises exception on failure"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = 'Internal Server Error'
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.get_business_term_metadata()
        
        assert 'Failed to fetch BusinessTerm data from Collibra' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_tables_success(self, mock_post, adapter, mock_logger):
        """Test get_tables returns table data on success"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'assets': [{'id': 't1', 'name': 'Table1'}]
            }
        }
        mock_post.return_value = mock_response
        
        result = adapter.get_tables()
        
        assert len(result) == 1
        assert result[0]['id'] == 't1'

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_business_term_hierarchy_success(self, mock_post, adapter, mock_logger):
        """Test get_business_term_hierarchy returns hierarchy data"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'assets': [{'id': '1', 'parent': '2'}]
            }
        }
        mock_post.return_value = mock_response
        
        result = adapter.get_business_term_hierarchy()
        
        assert len(result) == 1
        mock_logger.info.assert_called_with('Successfully fetched business term hierarchy from Collibra')

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_business_term_hierarchy_failure(self, mock_post, adapter):
        """Test get_business_term_hierarchy raises exception on failure"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = 'Not Found'
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.get_business_term_hierarchy()
        
        assert 'Failed to fetch business term hierarchy from Collibra' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_table_success(self, mock_post, adapter, mock_logger):
        """Test get_table returns single table"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'assets': [{'id': 'table-123', 'name': 'MyTable'}]
            }
        }
        mock_post.return_value = mock_response
        
        result = adapter.get_table('table-123')
        
        assert result['id'] == 'table-123'
        mock_logger.info.assert_called()

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_table_raises_error_when_multiple_results(self, mock_post, adapter):
        """Test get_table raises exception when multiple tables returned"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'assets': [{'id': '1'}, {'id': '2'}]
            }
        }
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.get_table('table-123')
        
        assert 'Failed to fetch table with id table-123' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_table_by_name_success(self, mock_post, adapter, mock_logger):
        """Test get_table_by_name returns table by name"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'assets': [{'id': 't1', 'name': 'customers'}]
            }
        }
        mock_post.return_value = mock_response
        
        result = adapter.get_table_by_name('customers')
        
        assert result['name'] == 'customers'
        mock_logger.info.assert_called()

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_table_by_name_raises_error_when_not_found(self, mock_post, adapter):
        """Test get_table_by_name raises exception when no table found"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'assets': []
            }
        }
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.get_table_by_name('nonexistent')
        
        assert 'No table found with name nonexistent' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_table_business_terms_success(self, mock_post, adapter, mock_logger):
        """Test get_table_business_terms returns business terms"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'assets': [{'id': 't1', 'terms': ['term1', 'term2']}]
            }
        }
        mock_post.return_value = mock_response
        
        result = adapter.get_table_business_terms('table-123')
        
        assert result['id'] == 't1'
        mock_logger.info.assert_called()

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_pii_columns_success(self, mock_post, adapter, mock_logger):
        """Test get_pii_columns returns PII column data"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'assets': [{'id': 't1', 'pii_columns': ['ssn', 'email']}]
            }
        }
        mock_post.return_value = mock_response
        
        result = adapter.get_pii_columns('table-123')
        
        assert result['id'] == 't1'
        mock_logger.info.assert_called()

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_pii_columns_raises_error_when_multiple_results(self, mock_post, adapter):
        """Test get_pii_columns raises exception when multiple results"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'assets': [{'id': '1'}, {'id': '2'}]
            }
        }
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.get_pii_columns('table-123')
        
        assert 'Failed to fetch PII columns for table with id table-123' in str(exc_info.value)



    @patch('adapter.CollibraAdapter.requests.post')
    def test_start_subscription_request_creation_workflow_success(self, mock_post, adapter):
        """Test start_subscription_request_creation_workflow creates workflow"""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {'workflowId': 'wf-123'}
        mock_post.return_value = mock_response
        
        result = adapter.start_subscription_request_creation_workflow('asset-123', 'project-name')
        
        assert result['workflowId'] == 'wf-123'
        # Verify REST API was called
        assert 'workflowInstances' in mock_post.call_args[0][0]

    @patch('adapter.CollibraAdapter.requests.post')
    def test_start_subscription_request_creation_workflow_failure(self, mock_post, adapter):
        """Test start_subscription_request_creation_workflow raises exception on failure"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = 'Bad Request'
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.start_subscription_request_creation_workflow('asset-123', 'project-name')
        
        assert 'Failed to start subscription workflow' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_subscription_requests_by_status_success(self, mock_post, adapter, mock_logger):
        """Test get_subscription_requests_by_status returns requests"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'assets': [{'id': 'req-1', 'status': 'Approved'}]
            }
        }
        mock_post.return_value = mock_response
        
        result = adapter.get_subscription_requests_by_status('Approved')
        
        assert len(result) == 1
        assert result[0]['status'] == 'Approved'
        mock_logger.info.assert_called()

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_or_create_aws_project_returns_existing(self, mock_post, adapter):
        """Test get_or_create_aws_project returns existing project"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'assets': [{'id': 'proj-123', 'name': 'MyProject'}]
            }
        }
        mock_post.return_value = mock_response
        
        result = adapter.get_or_create_aws_project('MyProject', 'smus-id-123')
        
        assert result['id'] == 'proj-123'

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_aws_project_success(self, mock_post, adapter, mock_logger):
        """Test get_aws_project returns project"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'assets': [{'id': 'proj-123', 'name': 'MyProject'}]
            }
        }
        mock_post.return_value = mock_response
        
        result = adapter.get_aws_project('MyProject')
        
        assert len(result) == 1
        assert result[0]['id'] == 'proj-123'
        mock_logger.info.assert_called()

    @patch('adapter.CollibraAdapter.requests.post')
    def test_create_aws_project_success(self, mock_post, adapter):
        """Test create_aws_project creates new project"""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {'id': 'proj-new', 'name': 'NewProject'}
        mock_post.return_value = mock_response
        
        result = adapter.create_aws_project('NewProject', 'smus-id-456')
        
        assert result['id'] == 'proj-new'

    @patch('adapter.CollibraAdapter.requests.post')
    def test_create_aws_project_failure(self, mock_post, adapter):
        """Test create_aws_project raises exception on failure"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.create_aws_project('NewProject', 'smus-id-456')
        
        assert 'Failed to create project with name NewProject' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.put')
    def test_add_aws_project_attributes_success(self, mock_put, adapter, mock_logger):
        """Test add_aws_project_attributes adds attributes"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'success': True}
        mock_put.return_value = mock_response
        
        result = adapter.add_aws_project_attributes('proj-123', 'smus-id-456')
        
        assert result['success'] is True
        mock_logger.info.assert_called()

    @patch('adapter.CollibraAdapter.requests.put')
    def test_add_aws_project_attributes_failure(self, mock_put, adapter):
        """Test add_aws_project_attributes raises exception on failure"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_put.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.add_aws_project_attributes('proj-123', 'smus-id-456')
        
        assert 'Failed to add project attribute' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_create_relation_success(self, mock_post, adapter):
        """Test create_relation creates relation between assets"""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {'id': 'rel-123'}
        mock_post.return_value = mock_response
        
        result = adapter.create_relation('source-1', 'target-1', 'relation-type-1')
        
        assert result['id'] == 'rel-123'

    @patch('adapter.CollibraAdapter.requests.post')
    def test_create_relation_failure(self, mock_post, adapter):
        """Test create_relation raises exception on failure"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = 'Bad Request'
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.create_relation('source-1', 'target-1', 'relation-type-1')
        
        assert 'Failed to create collibra asset relation' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_or_create_aws_user_returns_existing(self, mock_post, adapter):
        """Test get_or_create_aws_user returns existing user"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'assets': [{'id': 'user-123', 'name': 'testuser'}]
            }
        }
        mock_post.return_value = mock_response
        
        result = adapter.get_or_create_aws_user('testuser')
        
        assert result['id'] == 'user-123'

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_or_create_aws_user_creates_new_when_not_found(self, mock_post, adapter, mock_logger):
        """Test get_or_create_aws_user creates new user when not found"""
        # First call fails (user not found), second call succeeds (user created)
        mock_response_get = Mock()
        mock_response_get.status_code = 200
        mock_response_get.json.return_value = {'data': {'assets': []}}
        
        mock_response_create = Mock()
        mock_response_create.status_code = 201
        mock_response_create.json.return_value = {'id': 'user-new', 'name': 'newuser'}
        
        mock_post.side_effect = [mock_response_get, mock_response_create]
        
        result = adapter.get_or_create_aws_user('newuser')
        
        assert result['id'] == 'user-new'
        mock_logger.info.assert_called()

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_aws_user_success(self, mock_post, adapter, mock_logger):
        """Test get_aws_user returns user"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'assets': [{'id': 'user-123', 'name': 'testuser'}]
            }
        }
        mock_post.return_value = mock_response
        
        result = adapter.get_aws_user('testuser')
        
        assert result['id'] == 'user-123'
        mock_logger.info.assert_called()

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_aws_user_raises_error_when_not_found(self, mock_post, adapter):
        """Test get_aws_user raises exception when user not found"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {'assets': []}}
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.get_aws_user('nonexistent')
        
        assert 'Failed to fetch user with username nonexistent' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_create_aws_user_success(self, mock_post, adapter, mock_logger):
        """Test create_aws_user creates new user"""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {'id': 'user-new', 'name': 'newuser'}
        mock_post.return_value = mock_response
        
        result = adapter.create_aws_user('newuser')
        
        assert result['id'] == 'user-new'
        mock_logger.info.assert_called()

    @patch('adapter.CollibraAdapter.requests.post')
    def test_create_aws_user_failure(self, mock_post, adapter):
        """Test create_aws_user raises exception on failure"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = 'Bad Request'
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.create_aws_user('newuser')
        
        assert 'Failed to create user newuser' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_add_aws_user_attributes_success(self, mock_post, adapter):
        """Test add_aws_user_attributes adds attributes"""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {'success': True}
        mock_post.return_value = mock_response
        
        result = adapter.add_aws_user_attributes('user-123', 'project-name')
        
        assert result['success'] is True

    @patch('adapter.CollibraAdapter.requests.post')
    def test_add_aws_user_attributes_failure(self, mock_post, adapter):
        """Test add_aws_user_attributes raises exception on failure"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.add_aws_user_attributes('user-123', 'project-name')
        
        assert 'Failed to add attributes for user user-123' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.patch')
    def test_update_subscription_request_status_success(self, mock_patch, adapter):
        """Test update_subscription_request_status updates status"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'id': 'asset-id'}
        mock_patch.return_value = mock_response
        
        result = adapter.update_subscription_request_status('req-123', COLLIBRA_SUBSCRIPTION_REQUEST_GRANTED_STATUS_ID)
        
        assert result is not None

    @patch('adapter.CollibraAdapter.requests.patch')
    def test_update_subscription_request_status_failure(self, mock_patch, adapter):
        """Test update_subscription_request_status raises exception on failure"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_patch.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.update_subscription_request_status('req-123', COLLIBRA_SUBSCRIPTION_REQUEST_GRANTED_STATUS_ID)
        
        assert 'Failed to update subscription request status' in str(exc_info.value)

    # Additional exception tests for comprehensive coverage

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_tables_failure(self, mock_post, adapter):
        """Test get_tables raises exception on API failure"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = 'Internal Server Error'
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.get_tables()
        
        assert 'Failed to fetch Table data from Collibra' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_table_failure(self, mock_post, adapter):
        """Test get_table raises exception on API failure"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = 'Internal Server Error'
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.get_table('table-123')
        
        assert 'Failed to fetch table with id table-123 from Collibra' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_table_raises_error_when_no_results(self, mock_post, adapter):
        """Test get_table raises exception when no table found"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {'assets': []}}
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.get_table('table-123')
        
        assert 'Failed to fetch table with id table-123 from Collibra' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_table_by_name_failure(self, mock_post, adapter):
        """Test get_table_by_name raises exception on API failure"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = 'Internal Server Error'
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.get_table_by_name('customers')
        
        assert 'Failed to fetch table with name customers from Collibra' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_table_business_terms_failure(self, mock_post, adapter):
        """Test get_table_business_terms raises exception on API failure"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = 'Internal Server Error'
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.get_table_business_terms('table-123')
        
        assert 'Failed to fetch business terms of table with id table-123 from Collibra' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_pii_columns_failure(self, mock_post, adapter):
        """Test get_pii_columns raises exception on API failure"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = 'Internal Server Error'
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.get_pii_columns('table-123')
        
        assert 'Failed to fetch PII columns for table with id table-123 from Collibra' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_pii_columns_raises_error_when_no_results(self, mock_post, adapter):
        """Test get_pii_columns raises exception when no results"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {'assets': []}}
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.get_pii_columns('table-123')
        
        assert 'Failed to fetch PII columns for table with id table-123 from Collibra' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_subscription_requests_by_status_failure(self, mock_post, adapter):
        """Test get_subscription_requests_by_status raises exception on API failure"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = 'Internal Server Error'
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.get_subscription_requests_by_status('PENDING')
        
        assert 'Failed to fetch pending subscription requests from Collibra' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_aws_project_failure(self, mock_post, adapter):
        """Test get_aws_project raises exception on API failure"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = 'Internal Server Error'
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.get_aws_project('my-project')
        
        assert 'Failed to fetch asset with name my-project from Collibra' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_aws_user_failure(self, mock_post, adapter):
        """Test get_aws_user raises exception on API failure"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = 'Internal Server Error'
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.get_aws_user('testuser')
        
        assert 'Failed to fetch tuser with username testuser from Collibra' in str(exc_info.value)

    @patch('adapter.CollibraAdapter.requests.post')
    def test_get_aws_user_raises_error_when_multiple_results(self, mock_post, adapter):
        """Test get_aws_user raises exception when multiple users found"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'assets': [
                    {'id': 'user-1', 'name': 'testuser'},
                    {'id': 'user-2', 'name': 'testuser'}
                ]
            }
        }
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            adapter.get_aws_user('testuser')
        
        assert 'Failed to fetch user with username testuser from Collibra' in str(exc_info.value)
