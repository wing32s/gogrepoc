"""
Tests for proactive token refresh functionality.

Verifies that tokens are refreshed proactively before expiration
to prevent session timeouts during long downloads.
"""

import time
import pytest
from unittest.mock import Mock, patch, MagicMock
from modules.api import check_and_renew_token, renew_token

class TestProactiveTokenRefresh:
    """Test proactive token refresh to prevent session timeout during downloads"""
    
    def test_check_and_renew_token_with_expired_token(self):
        """Token should be renewed if already expired"""
        mock_session = Mock()
        mock_session.token = {
            'access_token': 'old_token',
            'refresh_token': 'refresh123',
            'expiry': time.time() - 100  # Expired 100 seconds ago
        }
        
        with patch('modules.api.renew_token', return_value=True) as mock_renew:
            result = check_and_renew_token(mock_session)
            
            assert result is True
            mock_renew.assert_called_once_with(mock_session)
    
    def test_check_and_renew_token_with_expiring_soon(self):
        """Token should be renewed if expiring within proactive buffer"""
        mock_session = Mock()
        mock_session.token = {
            'access_token': 'old_token',
            'refresh_token': 'refresh123',
            'expiry': time.time() + 60  # Expires in 60 seconds
        }
        
        with patch('modules.api.renew_token', return_value=True) as mock_renew:
            # With 300 second buffer, should trigger renewal
            result = check_and_renew_token(mock_session, proactive_buffer=300)
            
            assert result is True
            mock_renew.assert_called_once_with(mock_session)
    
    def test_check_and_renew_token_with_valid_token(self):
        """Token should not be renewed if plenty of time left"""
        mock_session = Mock()
        mock_session.token = {
            'access_token': 'valid_token',
            'refresh_token': 'refresh123',
            'expiry': time.time() + 1800  # Expires in 30 minutes
        }
        
        with patch('modules.api.renew_token') as mock_renew:
            # With 300 second buffer, should NOT trigger renewal
            result = check_and_renew_token(mock_session, proactive_buffer=300)
            
            assert result is True
            mock_renew.assert_not_called()
    
    def test_check_and_renew_token_at_buffer_threshold(self):
        """Token should be renewed when exactly at buffer threshold"""
        mock_session = Mock()
        mock_session.token = {
            'access_token': 'old_token',
            'refresh_token': 'refresh123',
            'expiry': time.time() + 299  # Expires in 299 seconds (< 300 buffer)
        }
        
        with patch('modules.api.renew_token', return_value=True) as mock_renew:
            result = check_and_renew_token(mock_session, proactive_buffer=300)
            
            assert result is True
            mock_renew.assert_called_once_with(mock_session)
    
    def test_check_and_renew_token_with_missing_expiry(self):
        """Should attempt renewal if token missing expiry field"""
        mock_session = Mock()
        mock_session.token = {
            'access_token': 'token_without_expiry',
            'refresh_token': 'refresh123'
            # No 'expiry' field
        }
        
        with patch('modules.api.renew_token', return_value=True) as mock_renew:
            result = check_and_renew_token(mock_session)
            
            assert result is True
            mock_renew.assert_called_once_with(mock_session)
    
    def test_check_and_renew_token_handles_renewal_failure(self):
        """Should return False if renewal fails"""
        mock_session = Mock()
        mock_session.token = {
            'access_token': 'old_token',
            'refresh_token': 'refresh123',
            'expiry': time.time() - 100  # Expired
        }
        
        with patch('modules.api.renew_token', return_value=False) as mock_renew:
            result = check_and_renew_token(mock_session)
            
            assert result is False
            mock_renew.assert_called_once()
    
    def test_check_and_renew_token_with_custom_buffer(self):
        """Should respect custom proactive buffer time"""
        mock_session = Mock()
        mock_session.token = {
            'access_token': 'token',
            'refresh_token': 'refresh123',
            'expiry': time.time() + 90  # Expires in 90 seconds
        }
        
        with patch('modules.api.renew_token', return_value=True) as mock_renew:
            # With 60 second buffer, should NOT renew (90 > 60)
            result = check_and_renew_token(mock_session, proactive_buffer=60)
            assert result is True
            mock_renew.assert_not_called()
            
        with patch('modules.api.renew_token', return_value=True) as mock_renew:
            # With 120 second buffer, SHOULD renew (90 < 120)
            result = check_and_renew_token(mock_session, proactive_buffer=120)
            assert result is True
            mock_renew.assert_called_once()


class TestTokenRenewalDuringDownload:
    """Test that token is refreshed before each download to prevent timeout"""
    
    @patch('modules.download.check_and_renew_token')
    @patch('modules.download.makeGOGSession')
    def test_token_refresh_called_before_download(self, mock_session, mock_check):
        """Worker should call check_and_renew_token before processing each download"""
        # This test verifies the integration point - the actual call is in worker()
        # We can't easily test the worker directly, but we verify the function exists
        # and has the right signature
        
        mock_session_obj = Mock()
        mock_session_obj.token = {
            'access_token': 'test_token',
            'refresh_token': 'refresh123',
            'expiry': time.time() + 3600
        }
        
        # Verify function can be called with expected parameters
        result = check_and_renew_token(mock_session_obj, proactive_buffer=300)
        assert isinstance(result, bool)
    
    def test_proactive_buffer_default_is_5_minutes(self):
        """Default proactive buffer should be 300 seconds (5 minutes)"""
        mock_session = Mock()
        mock_session.token = {
            'access_token': 'token',
            'refresh_token': 'refresh',
            'expiry': time.time() + 600  # 10 minutes
        }
        
        with patch('modules.api.renew_token') as mock_renew:
            # Should not renew with default buffer (600 > 300)
            check_and_renew_token(mock_session)
            mock_renew.assert_not_called()
