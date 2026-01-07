"""
Tests for multi-user support functionality.
"""

import pytest
import os
import shutil
import tempfile
from unittest.mock import Mock, patch, MagicMock


class TestUserPaths:
    """Test user path resolution."""
    
    def test_default_user_paths(self):
        """Test that default user uses root directory."""
        from modules.config import get_user_paths
        
        paths = get_user_paths(None)
        
        assert paths['token'] == 'gog-token.dat'
        assert paths['manifest'] == 'gog-manifest.dat'
        assert paths['resume_manifest'] == 'gog-resume-manifest.dat'
        assert paths['games_dir'] == 'games'
        assert paths['user_dir'] is None
    
    def test_named_user_paths(self, temp_dir):
        """Test that named users get subdirectories."""
        from modules.config import get_user_paths
        
        # Change to temp directory for test
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        try:
            paths = get_user_paths('alice')
            
            assert paths['token'] == os.path.join('users', 'alice', 'gog-token.dat')
            assert paths['manifest'] == os.path.join('users', 'alice', 'gog-manifest.dat')
            assert paths['resume_manifest'] == os.path.join('users', 'alice', 'gog-resume-manifest.dat')
            assert paths['games_dir'] == 'games'  # Shared!
            assert paths['user_dir'] == os.path.join('users', 'alice')
            
            # Verify directory was created
            assert os.path.exists(os.path.join(temp_dir, 'users', 'alice'))
        finally:
            os.chdir(original_cwd)
    
    def test_games_dir_always_shared(self, temp_dir):
        """Test that games directory is always 'games' regardless of user."""
        from modules.config import get_user_paths
        
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        try:
            paths_default = get_user_paths(None)
            paths_alice = get_user_paths('alice')
            paths_bob = get_user_paths('bob')
            
            # All users share the same games directory
            assert paths_default['games_dir'] == 'games'
            assert paths_alice['games_dir'] == 'games'
            assert paths_bob['games_dir'] == 'games'
        finally:
            os.chdir(original_cwd)


class TestUserValidation:
    """Test user ID validation."""
    
    def test_valid_simple_username(self):
        """Test that simple usernames are valid."""
        from modules.config import validate_user_id
        
        assert validate_user_id('alice') == True
        assert validate_user_id('bob123') == True
        assert validate_user_id('user_name') == True
    
    def test_none_is_valid(self):
        """Test that None (default user) is valid."""
        from modules.config import validate_user_id
        
        assert validate_user_id(None) == True
    
    def test_empty_string_invalid(self):
        """Test that empty strings are invalid."""
        from modules.config import validate_user_id
        
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_user_id('')
        
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_user_id('   ')
    
    def test_path_traversal_invalid(self):
        """Test that path traversal attempts are blocked."""
        from modules.config import validate_user_id
        
        with pytest.raises(ValueError, match="path separators"):
            validate_user_id('../etc')
        
        with pytest.raises(ValueError, match="path separators"):
            validate_user_id('..\\windows')
        
        with pytest.raises(ValueError, match="path separators"):
            validate_user_id('user/name')
    
    def test_reserved_names_invalid(self):
        """Test that Windows reserved names are blocked."""
        from modules.config import validate_user_id
        
        with pytest.raises(ValueError, match="reserved system name"):
            validate_user_id('CON')
        
        with pytest.raises(ValueError, match="reserved system name"):
            validate_user_id('PRN')
        
        with pytest.raises(ValueError, match="reserved system name"):
            validate_user_id('con')  # Case insensitive


class TestTokenMultiUser:
    """Test token save/load with multi-user support."""
    
    def test_save_load_token_default_user(self, temp_dir):
        """Test saving and loading token for default user."""
        from modules.api import save_token, load_token
        
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        try:
            test_token = {
                'access_token': 'test_access_123',
                'refresh_token': 'test_refresh_456',
                'expires_in': 3600
            }
            
            # Save without user_id (default user)
            save_token(test_token, user_id=None)
            
            # Should create token in root directory
            assert os.path.exists('gog-token.dat')
            
            # Load it back
            loaded = load_token(user_id=None)
            assert loaded['access_token'] == 'test_access_123'
            assert loaded['refresh_token'] == 'test_refresh_456'
        finally:
            os.chdir(original_cwd)
    
    def test_save_load_token_named_user(self, temp_dir):
        """Test saving and loading token for named user."""
        from modules.api import save_token, load_token
        
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        try:
            test_token = {
                'access_token': 'alice_access_789',
                'refresh_token': 'alice_refresh_012',
                'expires_in': 3600
            }
            
            # Save with user_id
            save_token(test_token, user_id='alice')
            
            # Should create token in user subdirectory
            expected_path = os.path.join('users', 'alice', 'gog-token.dat')
            assert os.path.exists(expected_path)
            
            # Load it back
            loaded = load_token(user_id='alice')
            assert loaded['access_token'] == 'alice_access_789'
            assert loaded['refresh_token'] == 'alice_refresh_012'
        finally:
            os.chdir(original_cwd)
    
    def test_multiple_users_separate_tokens(self, temp_dir):
        """Test that multiple users have separate token files."""
        from modules.api import save_token, load_token
        
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        try:
            alice_token = {'access_token': 'alice_token', 'expires_in': 3600}
            bob_token = {'access_token': 'bob_token', 'expires_in': 3600}
            
            save_token(alice_token, user_id='alice')
            save_token(bob_token, user_id='bob')
            
            # Both token files should exist
            assert os.path.exists(os.path.join('users', 'alice', 'gog-token.dat'))
            assert os.path.exists(os.path.join('users', 'bob', 'gog-token.dat'))
            
            # Load them back and verify they're different
            loaded_alice = load_token(user_id='alice')
            loaded_bob = load_token(user_id='bob')
            
            assert loaded_alice['access_token'] == 'alice_token'
            assert loaded_bob['access_token'] == 'bob_token'
        finally:
            os.chdir(original_cwd)


class TestListUsers:
    """Test user listing functionality."""
    
    def test_list_users_default_only(self, temp_dir):
        """Test listing when only default user exists."""
        from modules.config import list_users
        from modules.api import save_token
        
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        try:
            # Create default user token
            save_token({'access_token': 'test', 'expires_in': 3600}, user_id=None)
            
            users = list_users()
            assert None in users  # Default user
            assert len(users) == 1
        finally:
            os.chdir(original_cwd)
    
    def test_list_users_multiple(self, temp_dir):
        """Test listing multiple users."""
        from modules.config import list_users
        from modules.api import save_token
        
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        try:
            # Create multiple users
            save_token({'access_token': 'test', 'expires_in': 3600}, user_id=None)
            save_token({'access_token': 'alice', 'expires_in': 3600}, user_id='alice')
            save_token({'access_token': 'bob', 'expires_in': 3600}, user_id='bob')
            
            users = list_users()
            assert None in users
            assert 'alice' in users
            assert 'bob' in users
            assert len(users) == 3
        finally:
            os.chdir(original_cwd)
    
    def test_list_users_empty(self, temp_dir):
        """Test listing when no users exist."""
        from modules.config import list_users
        
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        try:
            users = list_users()
            assert users == []
        finally:
            os.chdir(original_cwd)


class TestCommandLineUserArgument:
    """Test --user command-line argument."""
    
    def test_parse_user_argument(self):
        """Test parsing --user argument."""
        from gogrepoc_new import process_argv
        
        # --user comes BEFORE the subcommand
        test_args = ['gogrepoc.py', '--user', 'alice', 'login']
        args = process_argv(test_args)
        
        assert hasattr(args, 'user')
        assert args.user == 'alice'
    
    def test_default_user_none(self):
        """Test that default user is None when --user not specified."""
        from gogrepoc_new import process_argv
        
        test_args = ['gogrepoc.py', 'login']
        args = process_argv(test_args)
        
        assert hasattr(args, 'user')
        assert args.user is None
    
    def test_user_argument_on_all_commands(self):
        """Test that --user works on all commands."""
        from gogrepoc_new import process_argv
        
        commands = ['login', 'update', 'download', 'verify']
        
        for cmd in commands:
            # --user comes BEFORE the subcommand
            test_args = ['gogrepoc.py', '--user', 'testuser', cmd]
            args = process_argv(test_args)
            assert args.user == 'testuser'
