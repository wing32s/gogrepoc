#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for MD5 lookup dictionary building functionality.
"""

import pytest
from modules.utils import build_md5_lookup, _add_to_md5_lookup, AttrDict
from modules.game_filter import GameFilter


class TestAddToMd5Lookup:
    """Test the _add_to_md5_lookup helper function."""
    
    def test_adds_item_to_empty_dict(self):
        """Should add item to empty dictionary."""
        size_info = {}
        item = AttrDict({
            'size': 1024,
            'md5': 'abc123',
            'name': 'test.exe'
        })
        
        _add_to_md5_lookup(size_info, item, 'game_folder')
        
        assert 1024 in size_info
        assert 'abc123' in size_info[1024]
        assert ('game_folder', 'test.exe') in size_info[1024]['abc123']
        assert size_info[1024]['abc123'][('game_folder', 'test.exe')] == item
    
    def test_adds_multiple_items_same_size(self):
        """Should handle multiple items with same size but different MD5."""
        size_info = {}
        item1 = AttrDict({'size': 1024, 'md5': 'abc123', 'name': 'file1.exe'})
        item2 = AttrDict({'size': 1024, 'md5': 'def456', 'name': 'file2.exe'})
        
        _add_to_md5_lookup(size_info, item1, 'game1')
        _add_to_md5_lookup(size_info, item2, 'game2')
        
        assert len(size_info[1024]) == 2
        assert 'abc123' in size_info[1024]
        assert 'def456' in size_info[1024]
    
    def test_adds_multiple_items_same_md5(self):
        """Should handle multiple items with same MD5 (different games)."""
        size_info = {}
        item1 = AttrDict({'size': 1024, 'md5': 'abc123', 'name': 'common.dll'})
        item2 = AttrDict({'size': 1024, 'md5': 'abc123', 'name': 'common.dll'})
        
        _add_to_md5_lookup(size_info, item1, 'game1')
        _add_to_md5_lookup(size_info, item2, 'game2')
        
        assert len(size_info[1024]['abc123']) == 2
        assert ('game1', 'common.dll') in size_info[1024]['abc123']
        assert ('game2', 'common.dll') in size_info[1024]['abc123']


class TestBuildMd5Lookup:
    """Test the build_md5_lookup function."""
    
    def test_builds_lookup_for_single_game(self):
        """Should build lookup dictionary for a single game."""
        gamesdb = [
            AttrDict({
                'id': 123,
                'title': 'test_game',
                'folder_name': 'test_game',
                'downloads': [
                    AttrDict({
                        'md5': 'abc123',
                        'size': 1024,
                        'name': 'setup.exe',
                        'lang': 'English',
                        'os_type': 'windows'
                    })
                ],
                'galaxyDownloads': [],
                'sharedDownloads': [],
                'extras': []
            })
        ]
        
        game_filter = GameFilter(
            os_list=['windows'],
            lang_list=['en']
        )
        
        result = build_md5_lookup(gamesdb, game_filter)
        
        assert 1024 in result
        assert 'abc123' in result[1024]
        assert ('test_game', 'setup.exe') in result[1024]['abc123']
    
    def test_filters_by_os(self):
        """Should only include items matching OS filter."""
        gamesdb = [
            AttrDict({
                'id': 123,
                'title': 'test_game',
                'folder_name': 'test_game',
                'downloads': [
                    AttrDict({
                        'md5': 'abc123',
                        'size': 1024,
                        'name': 'setup_windows.exe',
                        'lang': 'English',
                        'os_type': 'windows'
                    }),
                    AttrDict({
                        'md5': 'def456',
                        'size': 2048,
                        'name': 'setup_linux.sh',
                        'lang': 'English',
                        'os_type': 'linux'
                    })
                ],
                'galaxyDownloads': [],
                'sharedDownloads': [],
                'extras': []
            })
        ]
        
        game_filter = GameFilter(
            os_list=['windows'],
            lang_list=['en']
        )
        
        result = build_md5_lookup(gamesdb, game_filter)
        
        assert 1024 in result  # Windows file included
        assert 2048 not in result  # Linux file excluded
    
    def test_filters_by_language(self):
        """Should only include items matching language filter."""
        gamesdb = [
            AttrDict({
                'id': 123,
                'title': 'test_game',
                'folder_name': 'test_game',
                'downloads': [
                    AttrDict({
                        'md5': 'abc123',
                        'size': 1024,
                        'name': 'setup_en.exe',
                        'lang': 'English',
                        'os_type': 'windows'
                    }),
                    AttrDict({
                        'md5': 'def456',
                        'size': 2048,
                        'name': 'setup_fr.exe',
                        'lang': 'français',
                        'os_type': 'windows'
                    })
                ],
                'galaxyDownloads': [],
                'sharedDownloads': [],
                'extras': []
            })
        ]
        
        game_filter = GameFilter(
            os_list=['windows'],
            lang_list=['en']
        )
        
        result = build_md5_lookup(gamesdb, game_filter)
        
        assert 1024 in result  # English file included
        assert 2048 not in result  # French file excluded
    
    def test_filters_by_installer_type_standalone(self):
        """Should filter to standalone installers only."""
        gamesdb = [
            AttrDict({
                'id': 123,
                'title': 'test_game',
                'folder_name': 'test_game',
                'downloads': [
                    AttrDict({
                        'md5': 'abc123',
                        'size': 1024,
                        'name': 'setup.exe',
                        'lang': 'English',
                        'os_type': 'windows'
                    })
                ],
                'galaxyDownloads': [
                    AttrDict({
                        'md5': 'def456',
                        'size': 2048,
                        'name': 'galaxy_setup.exe',
                        'lang': 'English',
                        'os_type': 'windows'
                    })
                ],
                'sharedDownloads': [],
                'extras': []
            })
        ]
        
        game_filter = GameFilter(
            os_list=['windows'],
            lang_list=['en'],
            installers='standalone'
        )
        
        result = build_md5_lookup(gamesdb, game_filter)
        
        assert 1024 in result  # Standalone included
        assert 2048 not in result  # Galaxy excluded
    
    def test_filters_by_installer_type_galaxy(self):
        """Should filter to Galaxy installers only."""
        gamesdb = [
            AttrDict({
                'id': 123,
                'title': 'test_game',
                'folder_name': 'test_game',
                'downloads': [
                    AttrDict({
                        'md5': 'abc123',
                        'size': 1024,
                        'name': 'setup.exe',
                        'lang': 'English',
                        'os_type': 'windows'
                    })
                ],
                'galaxyDownloads': [
                    AttrDict({
                        'md5': 'def456',
                        'size': 2048,
                        'name': 'galaxy_setup.exe',
                        'lang': 'English',
                        'os_type': 'windows'
                    })
                ],
                'sharedDownloads': [],
                'extras': []
            })
        ]
        
        game_filter = GameFilter(
            os_list=['windows'],
            lang_list=['en'],
            installers='galaxy'
        )
        
        result = build_md5_lookup(gamesdb, game_filter)
        
        assert 1024 not in result  # Standalone excluded
        assert 2048 in result  # Galaxy included
    
    def test_includes_extras(self):
        """Should include extras by default."""
        gamesdb = [
            AttrDict({
                'id': 123,
                'title': 'test_game',
                'folder_name': 'test_game',
                'downloads': [],
                'galaxyDownloads': [],
                'sharedDownloads': [],
                'extras': [
                    AttrDict({
                        'md5': 'abc123',
                        'size': 512,
                        'name': 'manual.pdf',
                        'lang': '',
                        'os_type': 'extra'
                    })
                ]
            })
        ]
        
        game_filter = GameFilter(
            os_list=['windows'],
            lang_list=['en']
        )
        
        result = build_md5_lookup(gamesdb, game_filter)
        
        assert 512 in result
        assert ('test_game', 'manual.pdf') in result[512]['abc123']
    
    def test_skips_extras_when_requested(self):
        """Should skip extras when skip_extras is True."""
        gamesdb = [
            AttrDict({
                'id': 123,
                'title': 'test_game',
                'folder_name': 'test_game',
                'downloads': [],
                'galaxyDownloads': [],
                'sharedDownloads': [],
                'extras': [
                    AttrDict({
                        'md5': 'abc123',
                        'size': 512,
                        'name': 'manual.pdf',
                        'lang': '',
                        'os_type': 'extra'
                    })
                ]
            })
        ]
        
        game_filter = GameFilter(
            os_list=['windows'],
            lang_list=['en'],
            skip_extras=True
        )
        
        result = build_md5_lookup(gamesdb, game_filter)
        
        assert 512 not in result
    
    def test_filters_by_game_ids(self):
        """Should only include games matching ID filter."""
        gamesdb = [
            AttrDict({
                'id': 123,
                'title': 'game1',
                'folder_name': 'game1',
                'downloads': [
                    AttrDict({
                        'md5': 'abc123',
                        'size': 1024,
                        'name': 'setup.exe',
                        'lang': 'English',
                        'os_type': 'windows'
                    })
                ],
                'galaxyDownloads': [],
                'sharedDownloads': [],
                'extras': []
            }),
            AttrDict({
                'id': 456,
                'title': 'game2',
                'folder_name': 'game2',
                'downloads': [
                    AttrDict({
                        'md5': 'def456',
                        'size': 2048,
                        'name': 'setup.exe',
                        'lang': 'English',
                        'os_type': 'windows'
                    })
                ],
                'galaxyDownloads': [],
                'sharedDownloads': [],
                'extras': []
            })
        ]
        
        game_filter = GameFilter(
            os_list=['windows'],
            lang_list=['en'],
            ids=['game1']
        )
        
        result = build_md5_lookup(gamesdb, game_filter)
        
        assert 1024 in result  # game1 included
        assert 2048 not in result  # game2 excluded
    
    def test_excludes_skipped_games(self):
        """Should exclude games in skipids list."""
        gamesdb = [
            AttrDict({
                'id': 123,
                'title': 'game1',
                'folder_name': 'game1',
                'downloads': [
                    AttrDict({
                        'md5': 'abc123',
                        'size': 1024,
                        'name': 'setup.exe',
                        'lang': 'English',
                        'os_type': 'windows'
                    })
                ],
                'galaxyDownloads': [],
                'sharedDownloads': [],
                'extras': []
            }),
            AttrDict({
                'id': 456,
                'title': 'game2',
                'folder_name': 'game2',
                'downloads': [
                    AttrDict({
                        'md5': 'def456',
                        'size': 2048,
                        'name': 'setup.exe',
                        'lang': 'English',
                        'os_type': 'windows'
                    })
                ],
                'galaxyDownloads': [],
                'sharedDownloads': [],
                'extras': []
            })
        ]
        
        game_filter = GameFilter(
            os_list=['windows'],
            lang_list=['en'],
            skipids=['game2']
        )
        
        result = build_md5_lookup(gamesdb, game_filter)
        
        assert 1024 in result  # game1 included
        assert 2048 not in result  # game2 excluded
    
    def test_skips_items_without_md5(self):
        """Should skip items that don't have MD5 hashes."""
        gamesdb = [
            AttrDict({
                'id': 123,
                'title': 'test_game',
                'folder_name': 'test_game',
                'downloads': [
                    AttrDict({
                        'md5': None,
                        'size': 1024,
                        'name': 'patch.exe',
                        'lang': 'English',
                        'os_type': 'windows'
                    })
                ],
                'galaxyDownloads': [],
                'sharedDownloads': [],
                'extras': []
            })
        ]
        
        game_filter = GameFilter(
            os_list=['windows'],
            lang_list=['en']
        )
        
        result = build_md5_lookup(gamesdb, game_filter)
        
        assert 1024 not in result  # Item without MD5 excluded
    
    def test_handles_missing_folder_name(self):
        """Should use title as folder_name if not present."""
        gamesdb = [
            AttrDict({
                'id': 123,
                'title': 'test_game',
                'downloads': [
                    AttrDict({
                        'md5': 'abc123',
                        'size': 1024,
                        'name': 'setup.exe',
                        'lang': 'English',
                        'os_type': 'windows'
                    })
                ],
                'galaxyDownloads': [],
                'sharedDownloads': [],
                'extras': []
            })
        ]
        
        game_filter = GameFilter(
            os_list=['windows'],
            lang_list=['en']
        )
        
        result = build_md5_lookup(gamesdb, game_filter)
        
        assert ('test_game', 'setup.exe') in result[1024]['abc123']
    
    def test_returns_empty_dict_for_no_matches(self):
        """Should return empty dictionary when no items match filters."""
        gamesdb = [
            AttrDict({
                'id': 123,
                'title': 'test_game',
                'folder_name': 'test_game',
                'downloads': [
                    AttrDict({
                        'md5': 'abc123',
                        'size': 1024,
                        'name': 'setup.exe',
                        'lang': 'English',
                        'os_type': 'linux'
                    })
                ],
                'galaxyDownloads': [],
                'sharedDownloads': [],
                'extras': []
            })
        ]
        
        game_filter = GameFilter(
            os_list=['windows'],  # Filter for windows
            lang_list=['en']
        )
        
        result = build_md5_lookup(gamesdb, game_filter)
        
        assert result == {}
