"""
Configuration module for multi-user support.

Provides path resolution for user-specific files (tokens, manifests) 
while keeping the games directory shared across all users.
"""

import os

def get_user_paths(user_id=None):
    """Get paths for user-specific files and directories.
    
    Games directory is ALWAYS shared at root level to avoid duplicate downloads.
    Only manifests and tokens are per-user.
    
    Args:
        user_id: User identifier (e.g., 'alice'). If None, uses root directory
                 for backward compatibility with single-user setup.
        
    Returns:
        dict with keys:
            - 'token': Path to token file
            - 'manifest': Path to manifest file
            - 'resume_manifest': Path to resume manifest file
            - 'games_dir': Path to games directory (always 'games')
            - 'user_dir': Base directory for user files (None for default user)
    
    Examples:
        # Default user (backward compatible)
        >>> get_user_paths(None)
        {'token': 'gog-token.dat', 'manifest': 'gog-manifest.dat', ...}
        
        # Named user
        >>> get_user_paths('alice')
        {'token': 'users/alice/gog-token.dat', 'manifest': 'users/alice/gog-manifest.dat', ...}
    """
    if user_id is None:
        # Default: root directory (backward compatible with existing setup)
        return {
            'token': 'gog-token.dat',
            'manifest': 'gog-manifest.dat',
            'resume_manifest': 'gog-resume-manifest.dat',
            'games_dir': 'games',
            'user_dir': None
        }
    else:
        # Multi-user: separate manifests and tokens, shared games directory
        user_dir = os.path.join('users', user_id)
        os.makedirs(user_dir, exist_ok=True)
        
        return {
            'token': os.path.join(user_dir, 'gog-token.dat'),
            'manifest': os.path.join(user_dir, 'gog-manifest.dat'),
            'resume_manifest': os.path.join(user_dir, 'gog-resume-manifest.dat'),
            'games_dir': 'games',  # SHARED - same for everyone!
            'user_dir': user_dir
        }


def list_users():
    """List all configured user profiles.
    
    Returns:
        list of user IDs (strings). Always includes None (default user) if
        root-level token exists, plus any users with subdirectories.
    """
    users = []
    
    # Check for default user (root level token)
    if os.path.exists('gog-token.dat'):
        users.append(None)  # None represents default/root user
    
    # Check for named users in users/ directory
    users_dir = 'users'
    if os.path.exists(users_dir):
        for entry in os.listdir(users_dir):
            user_path = os.path.join(users_dir, entry)
            if os.path.isdir(user_path):
                # Check if user has a token file
                token_path = os.path.join(user_path, 'gog-token.dat')
                if os.path.exists(token_path):
                    users.append(entry)
    
    return users


def validate_user_id(user_id):
    """Validate that a user ID is safe to use as a directory name.
    
    Args:
        user_id: User identifier to validate
        
    Returns:
        bool: True if valid, False otherwise
        
    Raises:
        ValueError: If user_id contains invalid characters
    """
    if user_id is None:
        return True
    
    # Check for empty string
    if not user_id or not user_id.strip():
        raise ValueError("User ID cannot be empty")
    
    # Check for path traversal attempts
    if '..' in user_id or '/' in user_id or '\\' in user_id:
        raise ValueError("User ID cannot contain path separators or '..'")
    
    # Check for reserved names on Windows
    reserved_names = {'CON', 'PRN', 'AUX', 'NUL', 'COM1', 'COM2', 'COM3', 
                      'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9', 
                      'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 
                      'LPT7', 'LPT8', 'LPT9'}
    if user_id.upper() in reserved_names:
        raise ValueError(f"User ID '{user_id}' is a reserved system name")
    
    return True
