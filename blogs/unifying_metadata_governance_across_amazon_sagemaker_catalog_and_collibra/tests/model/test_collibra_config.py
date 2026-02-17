"""
Unit tests for lambda/model/CollibraConfig.py
"""
import pytest


from model.CollibraConfig import CollibraConfig


@pytest.mark.unit
class TestCollibraConfig:
    """Tests for CollibraConfig class"""

    def test_init_with_all_fields(self):
        """Test initialization with all required fields"""
        data = {
            "username": "test_user",
            "password": "test_pass",
            "url": "test.collibra.com"
        }
        
        config = CollibraConfig(data)
        
        assert config.username == "test_user"
        assert config.password == "test_pass"
        assert config.url == "test.collibra.com"

    def test_init_with_missing_fields(self):
        """Test initialization with missing fields returns None"""
        data = {"username": "test_user"}
        
        config = CollibraConfig(data)
        
        assert config.username == "test_user"
        assert config.password is None
        assert config.url is None

    def test_init_with_empty_dict(self):
        """Test initialization with empty dictionary"""
        data = {}
        
        config = CollibraConfig(data)
        
        assert config.username is None
        assert config.password is None
        assert config.url is None



    def test_init_with_extra_fields(self):
        """Test initialization ignores extra fields"""
        data = {
            "username": "test_user",
            "password": "test_pass",
            "url": "test.collibra.com",
            "extra_field": "extra_value"
        }
        
        config = CollibraConfig(data)
        
        assert config.username == "test_user"
        assert config.password == "test_pass"
        assert config.url == "test.collibra.com"
        assert not hasattr(config, 'extra_field')

    def test_init_with_none_values(self):
        """Test initialization with None values"""
        data = {
            "username": None,
            "password": None,
            "url": None
        }
        
        config = CollibraConfig(data)
        
        assert config.username is None
        assert config.password is None
        assert config.url is None

    def test_init_with_empty_strings(self):
        """Test initialization with empty strings"""
        data = {
            "username": "",
            "password": "",
            "url": ""
        }
        
        config = CollibraConfig(data)
        
        assert config.username == ""
        assert config.password == ""
        assert config.url == ""
