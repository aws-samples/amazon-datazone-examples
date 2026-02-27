"""
Unit tests for lambda/utils/env_utils.py
"""
import pytest
import os
from unittest.mock import patch

from utils.env_utils import EnvUtils


@pytest.mark.unit
class TestEnvUtils:
    """Tests for EnvUtils class"""

    def test_get_env_var_returns_value_when_exists(self, monkeypatch):
        """Test that get_env_var returns the environment variable value"""
        monkeypatch.setenv('TEST_VAR', 'test_value')
        
        result = EnvUtils.get_env_var('TEST_VAR')
        
        assert result == 'test_value'

    def test_get_env_var_raises_error_when_required_and_missing(self):
        """Test that EnvironmentError is raised when required var is missing"""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(EnvironmentError) as exc_info:
                EnvUtils.get_env_var('MISSING_VAR', required=True)
        
        assert "Missing required environment variable: MISSING_VAR" in str(exc_info.value)

    def test_get_env_var_returns_none_when_not_required_and_missing(self):
        """Test that None is returned when var is not required and missing"""
        with patch.dict(os.environ, {}, clear=True):
            result = EnvUtils.get_env_var('MISSING_VAR', required=False)
        
        assert result is None

    def test_get_env_var_returns_default_when_missing(self):
        """Test that default value is returned when var is missing"""
        with patch.dict(os.environ, {}, clear=True):
            result = EnvUtils.get_env_var('MISSING_VAR', required=False, default='default_value')
        
        assert result == 'default_value'

    def test_get_env_var_prefers_actual_value_over_default(self, monkeypatch):
        """Test that actual env var value is preferred over default"""
        monkeypatch.setenv('TEST_VAR', 'actual_value')
        
        result = EnvUtils.get_env_var('TEST_VAR', required=False, default='default_value')
        
        assert result == 'actual_value'

    def test_get_env_var_returns_default_when_required_false_and_missing(self):
        """Test that default is returned when required=False and var is missing"""
        with patch.dict(os.environ, {}, clear=True):
            result = EnvUtils.get_env_var('MISSING_VAR', required=False, default='my_default')
        
        assert result == 'my_default'

    def test_get_env_var_returns_default_when_required_true_and_default_provided(self):
        """Test that default is returned when required=True and default is provided"""
        with patch.dict(os.environ, {}, clear=True):
            result = EnvUtils.get_env_var('MISSING_VAR', required=True, default='default_value')
        
        assert result == 'default_value'

    def test_get_env_var_handles_empty_string_value(self, monkeypatch):
        """Test that empty string is returned as valid value"""
        monkeypatch.setenv('TEST_VAR', '')
        
        result = EnvUtils.get_env_var('TEST_VAR', required=True)
        
        assert result == ''

    def test_get_env_var_handles_whitespace_value(self, monkeypatch):
        """Test that whitespace is preserved in env var value"""
        monkeypatch.setenv('TEST_VAR', '  spaces  ')
        
        result = EnvUtils.get_env_var('TEST_VAR')
        
        assert result == '  spaces  '


@pytest.mark.unit
class TestEnvUtilsModuleLevelVariables:
    """Tests for module-level environment variable loading"""

    def test_module_loads_required_env_vars(self, mock_env_vars):
        """Test that module successfully loads all required environment variables"""
        # Re-import to trigger module-level code with mocked env vars
        import importlib
        import utils.env_utils as env_utils_module
        importlib.reload(env_utils_module)
        
        # Verify key variables are loaded
        assert env_utils_module.SMUS_DOMAIN_ID == 'test-domain-id'
        assert env_utils_module.SMUS_GLOSSARY_OWNER_PROJECT_ID == 'test-project-id'
        assert env_utils_module.COLLIBRA_CONFIG_SECRETS_NAME == 'test-secret'

    def test_module_uses_default_for_smus_region(self, mock_env_vars, monkeypatch):
        """Test that SMUS_REGION uses default value when not provided"""
        # Remove SMUS_REGION from env
        monkeypatch.delenv('SMUS_REGION', raising=False)
        
        # Re-import to trigger module-level code
        import importlib
        import utils.env_utils as env_utils_module
        importlib.reload(env_utils_module)
        
        assert env_utils_module.SMUS_REGION == 'us-east-1'

    def test_module_loads_optional_admin_role_arn(self, mock_env_vars):
        """Test that optional SMUS_COLLIBRA_INTEGRATION_ADMIN_ROLE_ARN is loaded"""
        import importlib
        import utils.env_utils as env_utils_module
        importlib.reload(env_utils_module)
        
        assert env_utils_module.SMUS_COLLIBRA_INTEGRATION_ADMIN_ROLE_ARN == 'arn:aws:iam::123456789012:role/TestRole'
