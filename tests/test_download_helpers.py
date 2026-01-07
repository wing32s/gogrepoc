"""
Unit tests for utility functions extracted from download module.

These tests validate the helper functions work correctly.
"""
import pytest
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from modules.download import megs, gigs


class TestSizeFormatting:
    """Tests for file size formatting functions."""
    
    @pytest.mark.parametrize("size,expected", [
        (0, "0.0MB"),
        (512, "0.0MB"),
        (1024, "0.0MB"),
        (1024 * 1024, "1.0MB"),
        (1024 * 1024 * 10, "10.0MB"),
        (1024 * 1024 * 100, "100.0MB"),
        (1024 * 1024 * 1023, "1023.0MB"),
    ])
    def test_megs(self, size, expected):
        """Test megabyte formatting."""
        assert megs(size) == expected
    
    @pytest.mark.parametrize("size,expected", [
        (0, "0.00GB"),
        (1024 * 1024, "0.00GB"),
        (1024 * 1024 * 1024, "1.00GB"),
        (1024 * 1024 * 1024 * 2, "2.00GB"),
        (1024 * 1024 * 1024 * 10, "10.00GB"),
        (int(1024 * 1024 * 1024 * 1.5), "1.50GB"),
    ])
    def test_gigs(self, size, expected):
        """Test gigabyte formatting."""
        assert gigs(size) == expected


class TestGameFiltering:
    """Tests for game filtering functions."""
    
    def test_filter_games_by_id_with_ids(self, sample_manifest):
        """Test filtering games by ID list."""
        from modules.download import filter_games_by_id
        
        result = filter_games_by_id(sample_manifest, ['test_game'], [])
        assert len(result) == 1
        assert result[0].title == 'test_game'
    
    def test_filter_games_by_id_no_match(self, sample_manifest):
        """Test filtering with non-matching ID."""
        from modules.download import filter_games_by_id
        
        # Should exit when no match found
        with pytest.raises(SystemExit) as exc_info:
            result = filter_games_by_id(sample_manifest, ['nonexistent'], [])
        assert exc_info.value.code == 1
    
    def test_filter_games_by_skipids(self, sample_manifest):
        """Test excluding games by ID."""
        from modules.download import filter_games_by_id
        
        # Should exit when all games skipped
        with pytest.raises(SystemExit) as exc_info:
            result = filter_games_by_id(sample_manifest, [], ['test_game'])
        assert exc_info.value.code == 1
    
    def test_filter_games_no_filters(self, sample_manifest):
        """Test that no filters returns all games."""
        from modules.download import filter_games_by_id
        
        result = filter_games_by_id(sample_manifest, [], [])
        assert len(result) == 1


class TestDownloadFiltering:
    """Tests for download filtering by OS and language."""
    
    def test_filter_by_os_windows(self, sample_game_item):
        """Test filtering for Windows only."""
        from modules.download import filter_downloads_by_os_and_lang
        
        downloads = filter_downloads_by_os_and_lang(
            sample_game_item.downloads,
            ['windows'],
            ['en']
        )
        assert len(downloads) == 1
        assert downloads[0].os_type == 'windows'
    
    def test_filter_by_os_linux(self, sample_game_item):
        """Test filtering for Linux (should find none)."""
        from modules.download import filter_downloads_by_os_and_lang
        
        downloads = filter_downloads_by_os_and_lang(
            sample_game_item.downloads,
            ['linux'],
            ['en']
        )
        assert len(downloads) == 0
    
    def test_filter_extras_not_affected_by_os(self, sample_game_item):
        """Test that extras are not filtered by OS (they should not be passed to this function)."""
        from modules.download import filter_downloads_by_os_and_lang
        
        # This function should only be called on installers, not extras
        # Extras have os_type='extra' and should be handled separately
        downloads = filter_downloads_by_os_and_lang(
            sample_game_item.downloads,
            ['linux'],  # No Linux installer
            ['en']
        )
        assert len(downloads) == 0
        # Extras are not passed to this function - they're handled separately
    
    def test_filter_by_language(self, sample_game_item):
        """Test filtering by language."""
        from modules.download import filter_downloads_by_os_and_lang
        
        # Add German version
        from modules.utils import AttrDict
        german_download = AttrDict()
        german_download.name = 'setup_test_game_de.exe'
        german_download.os_type = 'windows'
        german_download.lang = 'Deutsch'  # GOG uses full language names
        sample_game_item.downloads.append(german_download)
        
        downloads = filter_downloads_by_os_and_lang(
            sample_game_item.downloads,
            ['windows'],
            ['de']  # This gets converted to 'Deutsch' via LANG_TABLE
        )
        assert len(downloads) == 1
        assert downloads[0].lang == 'Deutsch'
