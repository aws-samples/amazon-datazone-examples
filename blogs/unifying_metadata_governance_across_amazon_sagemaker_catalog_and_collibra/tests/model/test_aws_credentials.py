"""
Unit tests for lambda/model/AWSCredentials.py
"""
import pytest

from model.AWSCredentials import AWSCredentials


@pytest.mark.unit
class TestAWSCredentials:
    """Tests for AWSCredentials class"""

    def test_init_with_all_fields(self):
        """Test initialization with all required fields"""
        credentials = {
            'AccessKeyId': 'AKIAIOSFODNN7EXAMPLE',
            'SecretAccessKey': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            'SessionToken': 'FwoGZXIvYXdzEBYaDH...ExampleToken'
        }
        
        aws_creds = AWSCredentials(credentials)
        
        assert aws_creds.access_key_id == 'AKIAIOSFODNN7EXAMPLE'
        assert aws_creds.secret_access_key == 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        assert aws_creds.session_token == 'FwoGZXIvYXdzEBYaDH...ExampleToken'

    def test_init_raises_error_when_access_key_missing(self):
        """Test initialization raises KeyError when AccessKeyId is missing"""
        credentials = {
            'SecretAccessKey': 'secret',
            'SessionToken': 'token'
        }
        
        with pytest.raises(KeyError):
            AWSCredentials(credentials)

    def test_init_raises_error_when_secret_key_missing(self):
        """Test initialization raises KeyError when SecretAccessKey is missing"""
        credentials = {
            'AccessKeyId': 'access_key',
            'SessionToken': 'token'
        }
        
        with pytest.raises(KeyError):
            AWSCredentials(credentials)

    def test_init_raises_error_when_session_token_missing(self):
        """Test initialization raises KeyError when SessionToken is missing"""
        credentials = {
            'AccessKeyId': 'access_key',
            'SecretAccessKey': 'secret'
        }
        
        with pytest.raises(KeyError):
            AWSCredentials(credentials)

    def test_properties_are_read_only(self):
        """Test that properties cannot be set directly"""
        credentials = {
            'AccessKeyId': 'access_key',
            'SecretAccessKey': 'secret',
            'SessionToken': 'token'
        }
        aws_creds = AWSCredentials(credentials)
        
        with pytest.raises(AttributeError):
            aws_creds.access_key_id = "new_key"
        
        with pytest.raises(AttributeError):
            aws_creds.secret_access_key = "new_secret"
        
        with pytest.raises(AttributeError):
            aws_creds.session_token = "new_token"

    def test_init_with_empty_strings(self):
        """Test initialization with empty string values"""
        credentials = {
            'AccessKeyId': '',
            'SecretAccessKey': '',
            'SessionToken': ''
        }
        
        aws_creds = AWSCredentials(credentials)
        
        assert aws_creds.access_key_id == ''
        assert aws_creds.secret_access_key == ''
        assert aws_creds.session_token == ''
