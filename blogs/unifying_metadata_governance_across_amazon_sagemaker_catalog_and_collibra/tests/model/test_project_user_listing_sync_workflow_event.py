"""
Unit tests for lambda/model/ProjectUserListingSyncWorkflowEvent.py
"""
import pytest
import json

from model.ProjectUserListingSyncWorkflowEvent import ProjectUserListingSyncWorkflowEvent


@pytest.mark.unit
class TestProjectUserListingSyncWorkflowEvent:
    """Tests for ProjectUserListingSyncWorkflowEvent class"""

    def test_init_with_next_project_token(self):
        """Test initialization with next_project_token"""
        event = {'next_project_token': 'token123'}
        
        workflow_event = ProjectUserListingSyncWorkflowEvent(event)
        
        assert workflow_event.next_project_token == 'token123'

    def test_init_without_next_project_token(self):
        """Test initialization without next_project_token defaults to None"""
        event = {}
        
        workflow_event = ProjectUserListingSyncWorkflowEvent(event)
        
        assert workflow_event.next_project_token is None

    def test_init_with_none_token(self):
        """Test initialization with None token"""
        event = {'next_project_token': None}
        
        workflow_event = ProjectUserListingSyncWorkflowEvent(event)
        
        assert workflow_event.next_project_token is None

    def test_setter_updates_token(self):
        """Test that setter updates the token value"""
        event = {'next_project_token': 'token123'}
        workflow_event = ProjectUserListingSyncWorkflowEvent(event)
        
        workflow_event.next_project_token = 'new_token456'
        
        assert workflow_event.next_project_token == 'new_token456'

    def test_setter_can_set_to_none(self):
        """Test that setter can set token to None"""
        event = {'next_project_token': 'token123'}
        workflow_event = ProjectUserListingSyncWorkflowEvent(event)
        
        workflow_event.next_project_token = None
        
        assert workflow_event.next_project_token is None

    def test_dict_method_returns_correct_structure(self):
        """Test that __dict__ method returns correct dictionary"""
        event = {'next_project_token': 'token123'}
        workflow_event = ProjectUserListingSyncWorkflowEvent(event)
        
        result = workflow_event.__dict__()
        
        assert result == {'next_project_token': 'token123'}
        assert isinstance(result, dict)

    def test_dict_method_with_none_token(self):
        """Test __dict__ method with None token"""
        event = {}
        workflow_event = ProjectUserListingSyncWorkflowEvent(event)
        
        result = workflow_event.__dict__()
        
        assert result == {'next_project_token': None}

    def test_str_method_returns_json_string(self):
        """Test that __str__ method returns JSON string"""
        event = {'next_project_token': 'token123'}
        workflow_event = ProjectUserListingSyncWorkflowEvent(event)
        
        result = str(workflow_event)
        
        assert result == '{"next_project_token": "token123"}'
        assert isinstance(result, str)
        # Verify it's valid JSON
        parsed = json.loads(result)
        assert parsed['next_project_token'] == 'token123'

    def test_str_method_with_none_token(self):
        """Test __str__ method with None token"""
        event = {}
        workflow_event = ProjectUserListingSyncWorkflowEvent(event)
        
        result = str(workflow_event)
        
        assert result == '{"next_project_token": null}'
        parsed = json.loads(result)
        assert parsed['next_project_token'] is None

    def test_init_ignores_extra_fields(self):
        """Test initialization ignores extra fields in event"""
        event = {
            'next_project_token': 'token123',
            'extra_field': 'extra_value',
            'another_field': 'another_value'
        }
        
        workflow_event = ProjectUserListingSyncWorkflowEvent(event)
        
        assert workflow_event.next_project_token == 'token123'
        assert not hasattr(workflow_event, 'extra_field')
        assert not hasattr(workflow_event, 'another_field')

    def test_empty_string_token(self):
        """Test with empty string token"""
        event = {'next_project_token': ''}
        
        workflow_event = ProjectUserListingSyncWorkflowEvent(event)
        
        assert workflow_event.next_project_token == ''
