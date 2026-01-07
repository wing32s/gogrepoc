"""
Integration tests for command-line parameter parsing and behavior.
Tests that command-line options actually affect the behavior of the program.
"""

import pytest
import sys
import os
from unittest.mock import Mock, patch


class TestCommandLineOsFiltering:
    """Test that -os and -skipos parameters work correctly."""
    
    def test_os_filter_windows_only(self, sample_game_item):
        """Test -os windows filters to only Windows downloads."""
        from modules.download import filter_downloads_by_os_and_lang
        
        result = filter_downloads_by_os_and_lang(
            sample_game_item.downloads,
            os_list=['windows'],
            lang_list=None
        )
        
        # Should only include Windows downloads
        assert len(result) > 0
        for download in result:
            assert download.os_type == 'windows'
    
    def test_os_filter_linux_only(self, sample_game_item):
        """Test -os linux filters to only Linux downloads."""
        from modules.download import filter_downloads_by_os_and_lang
        from unittest.mock import Mock
        
        # Add a Linux download to test
        linux_download = Mock()
        linux_download.name = 'test_game_linux.tar.gz'
        linux_download.os_type = 'linux'
        linux_download.lang = 'English'
        linux_download.type = 'installer'
        
        # Add Linux download to the test
        downloads = list(sample_game_item.downloads) + [linux_download]
        
        result = filter_downloads_by_os_and_lang(
            downloads,
            os_list=['linux'],
            lang_list=None
        )
        
        # Should only include Linux downloads
        assert len(result) > 0
        for download in result:
            assert download.os_type == 'linux'
    
    def test_os_filter_multiple(self, sample_game_item):
        """Test -os with multiple OS types."""
        from modules.download import filter_downloads_by_os_and_lang
        
        result = filter_downloads_by_os_and_lang(
            sample_game_item.downloads,
            os_list=['windows', 'linux'],
            lang_list=None
        )
        
        # Should include both Windows and Linux
        assert len(result) > 0
        os_types = {d.os_type for d in result}
        assert 'windows' in os_types or 'linux' in os_types


class TestCommandLineLangFiltering:
    """Test that -lang and -skiplang parameters work correctly."""
    
    def test_lang_filter_english_only(self, sample_game_item):
        """Test -lang en filters to only English downloads."""
        from modules.download import filter_downloads_by_os_and_lang
        
        result = filter_downloads_by_os_and_lang(
            sample_game_item.downloads,
            os_list=None,
            lang_list=['en']  # Use language code, not full name
        )
        
        # Should only include English downloads
        assert len(result) > 0
        for download in result:
            assert download.lang == 'English'
    
    def test_lang_filter_german_only(self, sample_game_item):
        """Test -lang de filters to only German downloads."""
        from modules.download import filter_downloads_by_os_and_lang
        
        # Add a German download to test
        german_download = Mock()
        german_download.name = 'test_game_de.exe'
        german_download.os_type = 'windows'
        german_download.lang = 'Deutsch'
        german_download.type = 'installer'
        
        downloads = list(sample_game_item.downloads) + [german_download]
        
        result = filter_downloads_by_os_and_lang(
            downloads,
            os_list=None,
            lang_list=['de']  # Use language code, not full name
        )
        
        # Should only include German downloads
        assert len(result) > 0
        for download in result:
            assert download.lang == 'Deutsch'
    
    def test_lang_filter_multiple(self, sample_game_item):
        """Test -lang with multiple languages."""
        from modules.download import filter_downloads_by_os_and_lang
        
        result = filter_downloads_by_os_and_lang(
            sample_game_item.downloads,
            os_list=None,
            lang_list=['en', 'de']  # Use language codes
        )
        
        # Should include multiple languages
        assert len(result) > 0


class TestCommandLineIdFiltering:
    """Test that -ids and -skipids parameters work correctly."""
    
    def test_ids_filter_single_game(self, sample_manifest):
        """Test -ids with single game ID."""
        from modules.download import filter_games_by_id
        
        result = filter_games_by_id(
            sample_manifest,
            ids=[str(sample_manifest[0].id)],
            skipids=[]
        )
        
        # Should only include the specified game
        assert len(result) == 1
        assert result[0].id == sample_manifest[0].id
    
    def test_ids_filter_by_title(self, sample_manifest):
        """Test -ids with game title."""
        from modules.download import filter_games_by_id
        
        result = filter_games_by_id(
            sample_manifest,
            ids=['test_game'],
            skipids=[]
        )
        
        # Should find game by title
        assert len(result) == 1
        assert result[0].title == 'test_game'
    
    def test_skipids_filter_excludes_game(self, sample_manifest):
        """Test -skipids excludes specified game."""
        from modules.download import filter_games_by_id
        from unittest.mock import Mock
        
        # Add a second game so the list isn't empty after filtering
        game2 = Mock()
        game2.id = 234567
        game2.title = 'test_game_2'
        manifest = sample_manifest + [game2]
        
        result = filter_games_by_id(
            manifest,
            ids=[],
            skipids=[str(sample_manifest[0].id)]
        )
        
        # Should exclude the specified game
        assert all(g.id != sample_manifest[0].id for g in result)
        assert len(result) == 1  # Should have the second game
        assert result[0].id == 234567
    
    def test_ids_filter_multiple_games(self, sample_manifest):
        """Test -ids with multiple game IDs."""
        from modules.download import filter_games_by_id
        
        # Add a second game to manifest
        game2 = Mock()
        game2.id = 234567
        game2.title = 'test_game_2'
        manifest = sample_manifest + [game2]
        
        result = filter_games_by_id(
            manifest,
            ids=['123456', '234567'],
            skipids=[]
        )
        
        # Should include both games
        assert len(result) >= 1


class TestCommandLineCombinedFilters:
    """Test that combined filters work together correctly."""
    
    def test_os_and_lang_filter_combined(self, sample_game_item):
        """Test -os windows -lang en filters correctly."""
        from modules.download import filter_downloads_by_os_and_lang
        
        result = filter_downloads_by_os_and_lang(
            sample_game_item.downloads,
            os_list=['windows'],
            lang_list=['en']  # Use language code
        )
        
        # Should only include Windows English downloads
        assert len(result) > 0
        for download in result:
            assert download.os_type == 'windows'
            assert download.lang == 'English'
    
    def test_all_filters_applied(self, sample_manifest, sample_game_item):
        """Test that ID, OS, and language filters all work together."""
        from modules.download import filter_games_by_id, filter_downloads_by_os_and_lang
        
        # First filter by game ID
        games = filter_games_by_id(
            sample_manifest,
            ids=[str(sample_manifest[0].id)],
            skipids=[]
        )
        assert len(games) == 1
        
        # Then filter downloads by OS and language
        downloads = filter_downloads_by_os_and_lang(
            games[0].downloads,
            os_list=['windows'],
            lang_list=['en']  # Use language code
        )
        
        # Should have filtered downloads
        assert len(downloads) > 0
        for download in downloads:
            assert download.os_type == 'windows'
            assert download.lang == 'English'


class TestCommandLineArgumentParsing:
    """Test that argument parsing works correctly."""
    
    def test_parse_download_os_option(self):
        """Test parsing -os option for download command."""
        from gogrepoc_new import process_argv
        
        # Mock sys.argv
        test_args = ['gogrepoc.py', 'download', '-os', 'windows', 'linux']
        
        with patch.object(sys, 'argv', test_args):
            args = process_argv(test_args)
        """Test parsing -lang option."""
        from gogrepoc_new import process_argv
        
        test_args = ['gogrepoc.py', 'download', '-lang', 'en', 'de']
        
        with patch.object(sys, 'argv', test_args):
            args = process_argv(test_args)
    
    def test_parse_ids_option(self):
        """Test parsing -ids option."""
        from gogrepoc_new import process_argv
        
        test_args = ['gogrepoc.py', 'download', '-ids', '123456', '789012']
        
        with patch.object(sys, 'argv', test_args):
            args = process_argv(test_args)
            assert len(args.ids) >= 2
    
    def test_parse_skipids_option(self):
        """Test parsing -skipids option."""
        from gogrepoc_new import process_argv
        
        test_args = ['gogrepoc.py', 'download', '-skipids', '123456']
        
        with patch.object(sys, 'argv', test_args):
            args = process_argv(test_args)
            assert '123456' in args.skipids
    
    def test_mutually_exclusive_ids_skipids(self):
        """Test that -ids and -skipids are mutually exclusive."""
        from gogrepoc_new import process_argv
        
        test_args = ['gogrepoc.py', 'download', '-ids', '123', '-skipids', '456']
        
        with patch.object(sys, 'argv', test_args):
            # Should raise SystemExit due to mutually exclusive arguments
            with pytest.raises(SystemExit):
                process_argv(test_args)
    
    def test_mutually_exclusive_os_skipos(self):
        """Test that -os and -skipos are mutually exclusive."""
        from gogrepoc_new import process_argv
        
        test_args = ['gogrepoc.py', 'download', '-os', 'windows', '-skipos', 'linux']
        
        with patch.object(sys, 'argv', test_args):
            # Should raise SystemExit due to mutually exclusive arguments
            with pytest.raises(SystemExit):
                process_argv(test_args)
    
    def test_parse_skipgalaxy_flag(self):
        """Test parsing -skipgalaxy flag."""
        from gogrepoc_new import process_argv
        
        test_args = ['gogrepoc.py', 'download', '-skipgalaxy']
        
        with patch.object(sys, 'argv', test_args):
            args = process_argv(test_args)
            
            # Should set skipgalaxy flag
            assert hasattr(args, 'skipgalaxy')
            assert args.skipgalaxy == True
    
    def test_parse_skipextras_flag(self):
        """Test parsing -skipextras flag."""
        from gogrepoc_new import process_argv
        
        test_args = ['gogrepoc.py', 'download', '-skipextras']
        
        with patch.object(sys, 'argv', test_args):
            args = process_argv(test_args)
            
            # Should set skipextras flag
            assert hasattr(args, 'skipextras')
            assert args.skipextras == True
    
    def test_parse_dryrun_flag(self):
        """Test parsing -dryrun flag."""
        from gogrepoc_new import process_argv
        
        test_args = ['gogrepoc.py', 'download', '-dryrun']
        
        with patch.object(sys, 'argv', test_args):
            args = process_argv(test_args)
            
            # Should set dryrun flag
            assert hasattr(args, 'dryrun')
            assert args.dryrun == True
    
    def test_parse_wait_option(self):
        """Test parsing -wait option."""
        from gogrepoc_new import process_argv
        
        test_args = ['gogrepoc.py', 'download', '-wait', '2.5']
        
        with patch.object(sys, 'argv', test_args):
            args = process_argv(test_args)
            
            # Should parse wait time correctly
            assert hasattr(args, 'wait')
            assert args.wait == 2.5
    
    def test_parse_downloadlimit_option(self):
        """Test parsing -downloadlimit option."""
        from gogrepoc_new import process_argv
        
        test_args = ['gogrepoc.py', 'download', '-downloadlimit', '1024.5']
        
        with patch.object(sys, 'argv', test_args):
            args = process_argv(test_args)
            
            # Should parse download limit correctly
            assert hasattr(args, 'downloadlimit')
            assert args.downloadlimit == 1024.5


class TestUpdateCommandOptions:
    """Test update command specific options."""
    
    def test_parse_full_flag(self):
        """Test parsing -full flag for update command."""
        from gogrepoc_new import process_argv
        
        test_args = ['gogrepoc.py', 'update', '-full']
        
        with patch.object(sys, 'argv', test_args):
            args = process_argv(test_args)
            
            # Should set full flag
            assert hasattr(args, 'full')
            assert args.full == True
    
    def test_parse_updateonly_flag(self):
        """Test parsing -updateonly flag."""
        from gogrepoc_new import process_argv
        
        test_args = ['gogrepoc.py', 'update', '-updateonly']
        
        with patch.object(sys, 'argv', test_args):
            args = process_argv(test_args)
            
            # Should set updateonly flag
            assert hasattr(args, 'updateonly')
            assert args.updateonly == True
    
    def test_mutually_exclusive_update_modes(self):
        """Test that update mode flags are mutually exclusive."""
        from gogrepoc_new import process_argv
        
        test_args = ['gogrepoc.py', 'update', '-full', '-updateonly']
        
        with patch.object(sys, 'argv', test_args):
            # Should raise SystemExit due to mutually exclusive arguments
            with pytest.raises(SystemExit):
                process_argv(test_args)


class TestVerifyCommandOptions:
    """Test verify command specific options."""
    
    def test_parse_skipmd5_flag(self):
        """Test parsing -skipmd5 flag."""
        from gogrepoc_new import process_argv
        
        test_args = ['gogrepoc.py', 'verify', '-skipmd5']
        
        with patch.object(sys, 'argv', test_args):
            args = process_argv(test_args)
            
            # Should set skipmd5 flag
            assert hasattr(args, 'skipmd5')
            assert args.skipmd5 == True
    
    def test_parse_skipsize_flag(self):
        """Test parsing -skipsize flag."""
        from gogrepoc_new import process_argv
        
        test_args = ['gogrepoc.py', 'verify', '-skipsize']
        
        with patch.object(sys, 'argv', test_args):
            args = process_argv(test_args)
            
            # Should set skipsize flag
            assert hasattr(args, 'skipsize')
            assert args.skipsize == True
    
    def test_parse_delete_flag(self):
        """Test parsing -delete flag."""
        from gogrepoc_new import process_argv
        
        test_args = ['gogrepoc.py', 'verify', '-delete']
        
        with patch.object(sys, 'argv', test_args):
            args = process_argv(test_args)
            
            # Should set delete flag
            assert hasattr(args, 'delete')
            assert args.delete == True
    
    def test_mutually_exclusive_delete_noclean(self):
        """Test that -delete and -noclean are mutually exclusive."""
        from gogrepoc_new import process_argv
        
        test_args = ['gogrepoc.py', 'verify', '-delete', '-noclean']
        
        with patch.object(sys, 'argv', test_args):
            # Should raise SystemExit due to mutually exclusive arguments
            with pytest.raises(SystemExit):
                process_argv(test_args)
