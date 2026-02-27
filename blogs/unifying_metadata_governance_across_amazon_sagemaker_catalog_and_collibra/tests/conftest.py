"""
Pytest configuration and shared fixtures for SMUS-Collibra Integration tests.
"""
import os
import sys
from unittest.mock import MagicMock

import pytest

# Set up environment variables BEFORE importing any lambda code
# This prevents import-time errors from env_utils.py
os.environ.setdefault('SMUS_DOMAIN_ID', 'test-domain-id')
os.environ.setdefault('SMUS_GLOSSARY_OWNER_PROJECT_ID', 'test-project-id')
os.environ.setdefault('SMUS_REGION', 'us-east-1')
os.environ.setdefault('SMUS_COLLIBRA_INTEGRATION_ADMIN_ROLE_ARN', 'arn:aws:iam::123456789012:role/TestRole')
os.environ.setdefault('COLLIBRA_CONFIG_SECRETS_NAME', 'test-secret')
os.environ.setdefault('COLLIBRA_SUBSCRIPTION_REQUEST_CREATION_WORKFLOW_ID', 'test-workflow-id')
os.environ.setdefault('COLLIBRA_SUBSCRIPTION_REQUEST_APPROVAL_WORKFLOW_ID', 'test-approval-workflow-id')
os.environ.setdefault('COLLIBRA_AWS_PROJECT_TYPE_ID', 'test-project-type-id')
os.environ.setdefault('COLLIBRA_AWS_PROJECT_DOMAIN_ID', 'test-project-domain-id')
os.environ.setdefault('COLLIBRA_AWS_PROJECT_ATTRIBUTE_TYPE_ID', 'test-project-attr-type-id')
os.environ.setdefault('COLLIBRA_AWS_PROJECT_TO_ASSET_RELATION_TYPE_ID', 'test-relation-type-id')
os.environ.setdefault('COLLIBRA_AWS_USER_TYPE_ID', 'test-user-type-id')
os.environ.setdefault('COLLIBRA_AWS_USER_DOMAIN_ID', 'test-user-domain-id')
os.environ.setdefault('COLLIBRA_AWS_USER_PROJECT_ATTRIBUTE_TYPE_ID', 'test-user-project-attr-type-id')
os.environ.setdefault('COLLIBRA_SUBSCRIPTION_REQUEST_REJECTED_STATUS_ID', 'test-rejected-status-id')
os.environ.setdefault('COLLIBRA_SUBSCRIPTION_REQUEST_GRANTED_STATUS_ID', 'test-granted-status-id')

# Add lambda directory to path for imports
lambda_path = os.path.join(os.path.dirname(__file__), '..', 'lambda')
sys.path.insert(0, lambda_path)


@pytest.fixture
def mock_logger():
    """Provides a mock logger for testing."""
    logger = MagicMock()
    return logger


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Sets up mock environment variables for testing."""
    env_vars = {
        'SMUS_DOMAIN_ID': 'test-domain-id',
        'SMUS_GLOSSARY_OWNER_PROJECT_ID': 'test-project-id',
        'SMUS_REGION': 'us-east-1',
        'SMUS_COLLIBRA_INTEGRATION_ADMIN_ROLE_ARN': 'arn:aws:iam::123456789012:role/TestRole',
        'COLLIBRA_CONFIG_SECRETS_NAME': 'test-secret',
        'COLLIBRA_SUBSCRIPTION_REQUEST_CREATION_WORKFLOW_ID': 'test-workflow-id',
        'COLLIBRA_SUBSCRIPTION_REQUEST_APPROVAL_WORKFLOW_ID': 'test-approval-workflow-id',
        'COLLIBRA_AWS_PROJECT_TYPE_ID': 'test-project-type-id',
        'COLLIBRA_AWS_PROJECT_DOMAIN_ID': 'test-project-domain-id',
        'COLLIBRA_AWS_PROJECT_ATTRIBUTE_TYPE_ID': 'test-project-attr-type-id',
        'COLLIBRA_AWS_PROJECT_TO_ASSET_RELATION_TYPE_ID': 'test-relation-type-id',
        'COLLIBRA_AWS_USER_TYPE_ID': 'test-user-type-id',
        'COLLIBRA_AWS_USER_DOMAIN_ID': 'test-user-domain-id',
        'COLLIBRA_AWS_USER_PROJECT_ATTRIBUTE_TYPE_ID': 'test-user-project-attr-type-id',
        'COLLIBRA_SUBSCRIPTION_REQUEST_REJECTED_STATUS_ID': 'test-rejected-status-id',
        'COLLIBRA_SUBSCRIPTION_REQUEST_GRANTED_STATUS_ID': 'test-granted-status-id',
    }
    
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    
    return env_vars
