"""
Unit tests for spaces_manager module.
"""

import pytest
from unittest.mock import patch, MagicMock
import sys
import os

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from utils.spaces_manager import get_spaces_credentials_status, get_spaces_client


class TestSpacesManager:
    """Test cases for spaces manager functions."""

    def test_get_spaces_credentials_status_all_missing(self):
        """Test credential status when all credentials are missing."""
        with patch('utils.spaces_manager.SPACES_ACCESS_KEY_ID', None), \
             patch('utils.spaces_manager.SPACES_SECRET_ACCESS_KEY', None), \
             patch('utils.spaces_manager.SPACES_BUCKET_NAME', None), \
             patch('utils.spaces_manager.SPACES_REGION', None):
            
            status = get_spaces_credentials_status()
            
            assert status['all_present'] is False
            assert len(status['missing']) == 4
            assert 'SPACES_ACCESS_KEY_ID' in status['missing']
            assert 'SPACES_SECRET_ACCESS_KEY' in status['missing']
            assert 'SPACES_BUCKET_NAME' in status['missing']
            assert 'SPACES_REGION' in status['missing']

    def test_get_spaces_credentials_status_all_present(self):
        """Test credential status when all credentials are present."""
        with patch('utils.spaces_manager.SPACES_ACCESS_KEY_ID', 'test_key'), \
             patch('utils.spaces_manager.SPACES_SECRET_ACCESS_KEY', 'test_secret'), \
             patch('utils.spaces_manager.SPACES_BUCKET_NAME', 'test_bucket'), \
             patch('utils.spaces_manager.SPACES_REGION', 'nyc3'):
            
            status = get_spaces_credentials_status()
            
            assert status['all_present'] is True
            assert len(status['missing']) == 0

    def test_get_spaces_credentials_status_partial(self):
        """Test credential status when some credentials are missing."""
        with patch('utils.spaces_manager.SPACES_ACCESS_KEY_ID', 'test_key'), \
             patch('utils.spaces_manager.SPACES_SECRET_ACCESS_KEY', None), \
             patch('utils.spaces_manager.SPACES_BUCKET_NAME', 'test_bucket'), \
             patch('utils.spaces_manager.SPACES_REGION', ''):
            
            status = get_spaces_credentials_status()
            
            assert status['all_present'] is False
            assert len(status['missing']) == 2
            assert 'SPACES_SECRET_ACCESS_KEY' in status['missing']
            assert 'SPACES_REGION' in status['missing']

    def test_spaces_region_validation_fallback(self):
        """Test that SPACES_REGION validation provides fallback."""
        with patch('utils.spaces_manager.SPACES_ACCESS_KEY_ID', 'test_key'), \
             patch('utils.spaces_manager.SPACES_SECRET_ACCESS_KEY', 'test_secret'), \
             patch('utils.spaces_manager.SPACES_BUCKET_NAME', 'test_bucket'), \
             patch('utils.spaces_manager.SPACES_REGION', None), \
             patch('utils.spaces_manager.boto3') as mock_boto3:
            
            mock_session = MagicMock()
            mock_client = MagicMock()
            mock_session.client.return_value = mock_client
            mock_boto3.session.Session.return_value = mock_session
            
            client = get_spaces_client()
            
            # Should have used fallback region
            mock_session.client.assert_called_once()
            call_args = mock_session.client.call_args
            assert call_args[1]['region_name'] == 'nyc3'

    def test_spaces_region_validation_empty_string(self):
        """Test that empty string SPACES_REGION gets fallback."""
        with patch('utils.spaces_manager.SPACES_ACCESS_KEY_ID', 'test_key'), \
             patch('utils.spaces_manager.SPACES_SECRET_ACCESS_KEY', 'test_secret'), \
             patch('utils.spaces_manager.SPACES_BUCKET_NAME', 'test_bucket'), \
             patch('utils.spaces_manager.SPACES_REGION', ''), \
             patch('utils.spaces_manager.boto3') as mock_boto3:
            
            mock_session = MagicMock()
            mock_client = MagicMock()
            mock_session.client.return_value = mock_client
            mock_boto3.session.Session.return_value = mock_session
            
            client = get_spaces_client()
            
            # Should have used fallback region
            mock_session.client.assert_called_once()
            call_args = mock_session.client.call_args
            assert call_args[1]['region_name'] == 'nyc3'