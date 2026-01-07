"""
Shared test fixtures for gogrepoc test suite.
"""
import pytest
import os
import sys
import tempfile
import shutil
from unittest.mock import Mock

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    tmpdir = tempfile.mkdtemp()
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)

@pytest.fixture
def mock_gog_session():
    """Create a mock GOG session with valid token."""
    import time
    session = Mock()
    session.token = {
        'access_token': 'test_token_12345',
        'refresh_token': 'test_refresh_67890',
        'expires_in': 3600,
        'expiry': int(time.time()) + 3600
    }
    session.headers = {
        'User-Agent': 'test-agent',
        'Authorization': 'Bearer test_token_12345'
    }
    session.get = Mock()
    session.post = Mock()
    session.head = Mock()
    return session

@pytest.fixture
def sample_game_item():
    """Create a sample game item for testing."""
    from modules.utils import AttrDict
    
    item = AttrDict()
    item.id = '1234567890'
    item.title = 'test_game'
    item.folder_name = 'test_game'
    item.downloads = []
    item.extras = []
    
    # Add a sample download
    download = AttrDict()
    download.name = 'setup_test_game_1.0.exe'
    download.href = 'https://www.gog.com/downloads/test_game/setup.exe'
    download.size = 1024 * 1024 * 100  # 100MB
    download.md5 = 'abc123def456'
    download.os_type = 'windows'
    download.lang = 'English'  # GOG uses full language names, not codes
    download.type = 'installer'
    item.downloads.append(download)
    
    # Add a sample extra
    extra = AttrDict()
    extra.name = 'manual.pdf'
    extra.href = 'https://www.gog.com/downloads/test_game/manual.pdf'
    extra.size = 1024 * 500  # 500KB
    extra.md5 = '789ghi012jkl'
    extra.os_type = 'extra'
    extra.lang = ''
    extra.type = 'extra'
    item.extras.append(extra)
    
    return item

@pytest.fixture
def sample_manifest(sample_game_item):
    """Create a sample manifest with test games."""
    return [sample_game_item]
