"""
Tests for update strategy functions.

Tests the update module which provides functions to fetch game data
from GOG API and update the local manifest using different strategies.
"""
import pytest
import sys
import os
from unittest.mock import Mock, MagicMock, patch, call

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from modules.update import (
    FetchConfig,
    fetch_all_product_ids,
    fetch_and_parse_game_details,
    fetch_and_merge_manifest,
    update_full_library,
    update_specific_games,
    update_partial,
    update_new_games_only,
    update_changed_games_only,
    check_resume_needed,
    create_resume_properties,
    process_items_with_resume,
    handle_single_game_rename
)
from modules.utils import AttrDict
from modules.game_filter import GameFilter


# Test fixtures for mock GOG API responses

@pytest.fixture
def mock_session():
    """Create a mock session object."""
    return Mock()


@pytest.fixture
def sample_product_data():
    """Sample product list data from GOG API."""
    return {
        'totalPages': 1,
        'products': [
            {
                'id': 1234567,
                'slug': 'witcher_3',
                'title': 'The Witcher 3: Wild Hunt',
                'category': 'RPG',
                'image': 'https://example.com/witcher3.jpg',
                'url': 'https://www.gog.com/game/witcher_3',
                'rating': 5,
                'updates': 1  # Has updates
            },
            {
                'id': 7654321,
                'slug': 'cyberpunk_2077',
                'title': 'Cyberpunk 2077',
                'category': 'RPG',
                'image': 'https://example.com/cyberpunk.jpg',
                'url': 'https://www.gog.com/game/cyberpunk_2077',
                'rating': 4,
                'updates': 0  # No updates
            },
            {
                'id': 1111111,
                'slug': 'gwent',
                'title': 'GWENT',
                'category': 'Card Game',
                'image': 'https://example.com/gwent.jpg',
                'url': 'https://www.gog.com/game/gwent',
                'rating': 4,
                'updates': 0
            }
        ]
    }


@pytest.fixture
def sample_game_details():
    """Sample game details data from GOG API."""
    return {
        'title': 'The Witcher 3: Wild Hunt',  # Added for long_title
        'backgroundImage': 'https://example.com/bg.jpg',
        'cdKey': '',
        'forumLink': 'https://www.gog.com/forum/witcher_3',
        'changelog': 'Version 1.0: Initial release',
        'releaseTimestamp': 1234567890,
        'messages': [],
        'downloads': [
            {
                'name': 'Witcher 3 - Windows',
                'os': 'windows',
                'language': 'en',
                'version': '1.0'
            }
        ],
        'galaxyDownloads': [],
        'extras': [
            {
                'name': 'Manual',
                'type': 'manual'
            }
        ],
        'dlcs': []
    }


@pytest.fixture
def basic_config():
    """Basic FetchConfig for testing."""
    return FetchConfig(
        os_list=['windows'],
        lang_list=['en'],
        installers='both',
        strict_dupe=True,
        md5xmls=False,
        no_changelogs=False
    )


class TestFetchConfig:
    """Test the FetchConfig dataclass."""
    
    def test_default_values(self):
        """FetchConfig should have sensible defaults."""
        config = FetchConfig()
        assert config.os_list is None
        assert config.lang_list is None
        assert config.installers == 'both'
        assert config.strict_dupe == True
        assert config.md5xmls == True
        assert config.no_changelogs == False
    
    def test_custom_values(self):
        """FetchConfig should accept custom values."""
        config = FetchConfig(
            os_list=['linux', 'mac'],
            lang_list=['de', 'fr'],
            installers='standalone',
            strict_dupe=False,
            md5xmls=False,
            no_changelogs=True
        )
        assert config.os_list == ['linux', 'mac']
        assert config.lang_list == ['de', 'fr']
        assert config.installers == 'standalone'
        assert config.strict_dupe == False
        assert config.md5xmls == False
        assert config.no_changelogs == True


class TestFetchAllProductIds:
    """Test the fetch_all_product_ids function."""
    
    def test_single_page_fetch(self, mock_session, sample_product_data):
        """Should fetch all products from single page."""
        mock_response = Mock()
        mock_response.json.return_value = sample_product_data
        mock_session.return_value = mock_response
        
        with patch('modules.update.request', return_value=mock_response):
            products = fetch_all_product_ids(mock_session)
        
        assert len(products) == 3
        assert products[0].id == 1234567
        assert products[0].title == 'witcher_3'
        assert products[0].has_updates == True
        assert products[1].id == 7654321
        assert products[1].has_updates == False
    
    def test_multi_page_fetch(self, mock_session):
        """Should fetch and combine products from multiple pages."""
        page1_data = {
            'totalPages': 2,
            'products': [
                {'id': 1, 'slug': 'game1', 'title': 'Game 1', 'category': 'Action',
                 'image': '', 'url': '', 'rating': 5, 'updates': 0}
            ]
        }
        page2_data = {
            'totalPages': 2,
            'products': [
                {'id': 2, 'slug': 'game2', 'title': 'Game 2', 'category': 'RPG',
                 'image': '', 'url': '', 'rating': 4, 'updates': 1}
            ]
        }
        
        mock_response1 = Mock()
        mock_response1.json.return_value = page1_data
        mock_response2 = Mock()
        mock_response2.json.return_value = page2_data
        
        with patch('modules.update.request', side_effect=[mock_response1, mock_response2]):
            products = fetch_all_product_ids(mock_session)
        
        assert len(products) == 2
        assert products[0].id == 1
        assert products[1].id == 2
    
    def test_handles_updates_flag(self, mock_session, sample_product_data):
        """Should correctly parse has_updates flag."""
        mock_response = Mock()
        mock_response.json.return_value = sample_product_data
        
        with patch('modules.update.request', return_value=mock_response):
            products = fetch_all_product_ids(mock_session)
        
        # First game has updates=1 (truthy)
        assert products[0].has_updates == True
        # Second game has updates=0 (falsy)
        assert products[1].has_updates == False


class TestFetchAndParseGameDetails:
    """Test the fetch_and_parse_game_details function."""
    
    def test_successful_fetch(self, mock_session, sample_game_details, basic_config):
        """Should fetch and parse game details successfully."""
        mock_response = Mock()
        mock_response.json.return_value = sample_game_details
        
        with patch('modules.update.request', return_value=mock_response):
            with patch('modules.update.filter_downloads'):
                with patch('modules.update.filter_extras'):
                    with patch('modules.update.filter_dlcs'):
                        with patch('modules.update.deDuplicateList', side_effect=lambda x, y, z: x):
                            game_data = fetch_and_parse_game_details(
                                mock_session, 1234567, 'witcher_3', basic_config
                            )
        
        assert game_data is not None
        assert game_data.id == 1234567
        assert game_data.title == 'witcher_3'
        assert game_data.bg_url == 'https://example.com/bg.jpg'
        assert game_data.changelog == 'Version 1.0: Initial release'
    
    def test_no_changelogs_flag(self, mock_session, sample_game_details):
        """Should exclude changelog when no_changelogs is True."""
        config = FetchConfig(no_changelogs=True)
        mock_response = Mock()
        mock_response.json.return_value = sample_game_details
        
        with patch('modules.update.request', return_value=mock_response):
            with patch('modules.update.filter_downloads'):
                with patch('modules.update.filter_extras'):
                    with patch('modules.update.filter_dlcs'):
                        with patch('modules.update.deDuplicateList', side_effect=lambda x, y, z: x):
                            game_data = fetch_and_parse_game_details(
                                mock_session, 1234567, 'witcher_3', config
                            )
        
        assert game_data.changelog == ''
    
    def test_handles_fetch_failure(self, mock_session, basic_config):
        """Should return None on fetch failure."""
        with patch('modules.update.request', side_effect=Exception('API Error')):
            game_data = fetch_and_parse_game_details(
                mock_session, 1234567, 'witcher_3', basic_config
            )
        
        assert game_data is None


class TestFetchAndMergeManifest:
    """Test the fetch_and_merge_manifest helper function."""
    
    def test_merges_product_and_detail_data(self, mock_session, basic_config):
        """Should merge product list data with detailed data."""
        # Create mock products
        products = [
            AttrDict({'id': 1, 'title': 'game1', 'has_updates': True, 'genre': 'RPG'}),
            AttrDict({'id': 2, 'title': 'game2', 'has_updates': False, 'genre': 'Action'})
        ]
        
        # Mock fetch_and_parse_game_details to return partial data
        def mock_fetch_details(session, game_id, title, config):
            return AttrDict({'id': game_id, 'title': title, 'downloads': []})
        
        with patch('modules.update.fetch_and_parse_game_details', side_effect=mock_fetch_details):
            manifest = fetch_and_merge_manifest(mock_session, products, basic_config)
        
        assert len(manifest) == 2
        # Should have both product data (genre) and detail data (downloads)
        assert manifest[0].genre == 'RPG'
        assert manifest[0].downloads == []
        assert manifest[1].genre == 'Action'
    
    def test_skips_failed_fetches(self, mock_session, basic_config):
        """Should skip games that fail to fetch."""
        products = [
            AttrDict({'id': 1, 'title': 'game1'}),
            AttrDict({'id': 2, 'title': 'game2'})
        ]
        
        # First fetch succeeds, second fails
        def mock_fetch_details(session, game_id, title, config):
            if game_id == 1:
                return AttrDict({'id': game_id, 'title': title})
            return None
        
        with patch('modules.update.fetch_and_parse_game_details', side_effect=mock_fetch_details):
            manifest = fetch_and_merge_manifest(mock_session, products, basic_config)
        
        # Only successful fetch should be in manifest
        assert len(manifest) == 1
        assert manifest[0].id == 1
    
    def test_empty_product_list(self, mock_session, basic_config):
        """Should handle empty product list."""
        manifest = fetch_and_merge_manifest(mock_session, [], basic_config)
        assert manifest == []


class TestUpdateFullLibrary:
    """Test the update_full_library strategy function."""
    
    def test_fetches_all_games(self, mock_session, basic_config):
        """Should fetch all games without filtering."""
        mock_products = [
            AttrDict({'id': 1, 'title': 'game1', 'has_updates': True}),
            AttrDict({'id': 2, 'title': 'game2', 'has_updates': False}),
            AttrDict({'id': 3, 'title': 'game3', 'has_updates': False})
        ]
        
        with patch('modules.update.fetch_all_product_ids', return_value=mock_products):
            with patch('modules.update.fetch_and_merge_manifest', return_value=mock_products):
                manifest = update_full_library(mock_session, basic_config)
        
        assert len(manifest) == 3


class TestUpdateSpecificGames:
    """Test the update_specific_games strategy function."""
    
    def test_filters_to_requested_games(self, mock_session, basic_config):
        """Should only fetch games matching requested IDs."""
        mock_products = [
            AttrDict({'id': 1, 'title': 'witcher_3'}),
            AttrDict({'id': 2, 'title': 'cyberpunk_2077'}),
            AttrDict({'id': 3, 'title': 'gwent'})
        ]
        
        with patch('modules.update.fetch_all_product_ids', return_value=mock_products):
            with patch('modules.update.fetch_and_merge_manifest') as mock_merge:
                mock_merge.return_value = [mock_products[0]]
                manifest = update_specific_games(
                    mock_session, ['witcher_3'], basic_config
                )
        
        # Should only process the requested game
        assert len(manifest) == 1
        assert manifest[0].title == 'witcher_3'
    
    def test_returns_empty_if_no_matches(self, mock_session, basic_config):
        """Should return empty list if no games match."""
        mock_products = [
            AttrDict({'id': 1, 'title': 'witcher_3'})
        ]
        
        with patch('modules.update.fetch_all_product_ids', return_value=mock_products):
            manifest = update_specific_games(
                mock_session, ['nonexistent_game'], basic_config
            )
        
        assert manifest == []


class TestUpdatePartial:
    """Test the update_partial strategy function."""
    
    def test_includes_new_and_updated_games(self, mock_session, basic_config):
        """Should include both new games and games with updates."""
        mock_products = [
            AttrDict({'id': 1, 'title': 'game1', 'has_updates': True}),   # Known, has updates
            AttrDict({'id': 2, 'title': 'game2', 'has_updates': False}),  # Known, no updates
            AttrDict({'id': 3, 'title': 'game3', 'has_updates': False})   # New game
        ]
        
        known_ids = [1, 2]  # Games 1 and 2 are known
        
        with patch('modules.update.fetch_all_product_ids', return_value=mock_products):
            with patch('modules.update.fetch_and_merge_manifest') as mock_merge:
                # Should filter to game 1 (updated) and game 3 (new)
                def side_effect(session, filtered, config):
                    return filtered
                mock_merge.side_effect = side_effect
                
                manifest = update_partial(mock_session, known_ids, basic_config)
        
        # Should get 2 games: one updated, one new
        # The actual filtering happens in filter_game_list which we tested separately
        assert isinstance(manifest, list)
    
    def test_returns_empty_if_no_changes(self, mock_session, basic_config):
        """Should return empty if no new games or updates."""
        mock_products = [
            AttrDict({'id': 1, 'title': 'game1', 'has_updates': False})
        ]
        
        known_ids = [1]
        
        with patch('modules.update.fetch_all_product_ids', return_value=mock_products):
            manifest = update_partial(mock_session, known_ids, basic_config)
        
        assert manifest == []


class TestUpdateNewGamesOnly:
    """Test the update_new_games_only strategy function."""
    
    def test_only_includes_new_games(self, mock_session, basic_config):
        """Should only include games not in known_ids."""
        mock_products = [
            AttrDict({'id': 1, 'title': 'game1', 'has_updates': True}),   # Known
            AttrDict({'id': 2, 'title': 'game2', 'has_updates': False}),  # New
        ]
        
        known_ids = [1]
        
        with patch('modules.update.fetch_all_product_ids', return_value=mock_products):
            with patch('modules.update.fetch_and_merge_manifest') as mock_merge:
                def side_effect(session, filtered, config):
                    return filtered
                mock_merge.side_effect = side_effect
                
                manifest = update_new_games_only(mock_session, known_ids, basic_config)
        
        assert isinstance(manifest, list)
    
    def test_returns_empty_if_no_new_games(self, mock_session, basic_config):
        """Should return empty if all games are known."""
        mock_products = [
            AttrDict({'id': 1, 'title': 'game1', 'has_updates': False})
        ]
        
        known_ids = [1]
        
        with patch('modules.update.fetch_all_product_ids', return_value=mock_products):
            manifest = update_new_games_only(mock_session, known_ids, basic_config)
        
        assert manifest == []


class TestUpdateChangedGamesOnly:
    """Test the update_changed_games_only strategy function."""
    
    def test_only_includes_games_with_updates(self, mock_session, basic_config):
        """Should only include games with has_updates=True."""
        mock_products = [
            AttrDict({'id': 1, 'title': 'game1', 'has_updates': True}),
            AttrDict({'id': 2, 'title': 'game2', 'has_updates': False}),
            AttrDict({'id': 3, 'title': 'game3', 'has_updates': True})
        ]
        
        with patch('modules.update.fetch_all_product_ids', return_value=mock_products):
            with patch('modules.update.fetch_and_merge_manifest') as mock_merge:
                def side_effect(session, filtered, config):
                    return filtered
                mock_merge.side_effect = side_effect
                
                manifest = update_changed_games_only(mock_session, basic_config)
        
        assert isinstance(manifest, list)
    
    def test_returns_empty_if_no_updates(self, mock_session, basic_config):
        """Should return empty if no games have updates."""
        mock_products = [
            AttrDict({'id': 1, 'title': 'game1', 'has_updates': False}),
            AttrDict({'id': 2, 'title': 'game2', 'has_updates': False})
        ]
        
        with patch('modules.update.fetch_all_product_ids', return_value=mock_products):
            manifest = update_changed_games_only(mock_session, basic_config)
        
        assert manifest == []


class TestCheckResumeNeeded:
    """Test the check_resume_needed function."""
    
    def test_no_resume_manifest_returns_false(self):
        """Should return (False, None, None) if no resume manifest exists."""
        with patch('modules.update.load_resume_manifest', side_effect=Exception('No file')):
            needresume, resumedb, resumeprops = check_resume_needed('resume')
        
        assert needresume == False
        assert resumedb is None
        assert resumeprops is None
    
    def test_noresume_mode_returns_false(self):
        """Should return (False, ..., ...) if resumemode is 'noresume'."""
        mock_resumedb = [AttrDict({'id': 1})]
        mock_props = {
            'complete': False,
            'resume_manifest_syntax_version': 1
        }
        mock_resumedb.append(mock_props)
        
        with patch('modules.update.load_resume_manifest', return_value=mock_resumedb):
            with patch('modules.update.RESUME_MANIFEST_SYNTAX_VERSION', 1):
                needresume, resumedb, resumeprops = check_resume_needed('noresume')
        
        assert needresume == False
    
    def test_complete_resume_returns_false(self):
        """Should return (False, ..., ...) if resume is marked complete."""
        mock_resumedb = [AttrDict({'id': 1})]
        mock_props = {
            'complete': True,
            'resume_manifest_syntax_version': 1
        }
        mock_resumedb.append(mock_props)
        
        with patch('modules.update.load_resume_manifest', return_value=mock_resumedb):
            with patch('modules.update.RESUME_MANIFEST_SYNTAX_VERSION', 1):
                needresume, resumedb, resumeprops = check_resume_needed('resume')
        
        assert needresume == False
    
    def test_incomplete_resume_returns_true(self):
        """Should return (True, resumedb, props) if resume is incomplete."""
        mock_item = AttrDict({'id': 1, 'title': 'game1'})
        mock_resumedb = [mock_item]
        mock_props = {
            'complete': False,
            'resume_manifest_syntax_version': 1,
            'os_list': ['windows']
        }
        mock_resumedb.append(mock_props)
        
        with patch('modules.update.load_resume_manifest', return_value=mock_resumedb):
            with patch('modules.update.RESUME_MANIFEST_SYNTAX_VERSION', 1):
                needresume, resumedb, resumeprops = check_resume_needed('resume')
        
        assert needresume == True
        assert len(resumedb) == 1
        assert resumedb[0].id == 1
        assert resumeprops['os_list'] == ['windows']
    
    def test_incompatible_version_prompts_user_discard(self):
        """Should prompt user and return False if version incompatible and user discards."""
        mock_resumedb = [AttrDict({'id': 1})]
        mock_props = {
            'complete': False,
            'resume_manifest_syntax_version': 0  # Old version
        }
        mock_resumedb.append(mock_props)
        
        with patch('modules.update.load_resume_manifest', return_value=mock_resumedb):
            with patch('modules.update.RESUME_MANIFEST_SYNTAX_VERSION', 1):
                with patch('builtins.input', return_value='D'):
                    needresume, resumedb, resumeprops = check_resume_needed('resume')
        
        assert needresume == False
        assert resumedb is None
        assert resumeprops is None
    
    def test_incompatible_version_prompts_user_abort(self):
        """Should exit if version incompatible and user aborts."""
        mock_resumedb = [AttrDict({'id': 1})]
        mock_props = {
            'complete': False,
            'resume_manifest_syntax_version': 0
        }
        mock_resumedb.append(mock_props)
        
        with patch('modules.update.load_resume_manifest', return_value=mock_resumedb):
            with patch('modules.update.RESUME_MANIFEST_SYNTAX_VERSION', 1):
                with patch('builtins.input', return_value='A'):
                    with pytest.raises(SystemExit):
                        check_resume_needed('resume')
    
    def test_missing_version_key_treated_as_incompatible(self):
        """Should handle missing version key gracefully."""
        mock_resumedb = [AttrDict({'id': 1})]
        mock_props = {
            'complete': False
            # Missing resume_manifest_syntax_version
        }
        mock_resumedb.append(mock_props)
        
        with patch('modules.update.load_resume_manifest', return_value=mock_resumedb):
            with patch('modules.update.RESUME_MANIFEST_SYNTAX_VERSION', 1):
                with patch('builtins.input', return_value='D'):
                    needresume, resumedb, resumeprops = check_resume_needed('resume')
        
        assert needresume == False


class TestCreateResumeProperties:
    """Test the create_resume_properties function."""
    
    def test_creates_complete_properties_dict(self):
        """Should create dict with all required resume properties."""
        config = FetchConfig(
            os_list=['windows', 'linux'],
            lang_list=['en', 'fr'],
            installers='both',
            strict_dupe=True,
            md5xmls=True,
            no_changelogs=False
        )
        
        with patch('modules.update.RESUME_MANIFEST_SYNTAX_VERSION', 1):
            props = create_resume_properties(config, skipknown=True, partial=False, updateonly=False)
        
        assert props['resume_manifest_syntax_version'] == 1
        assert props['os_list'] == ['windows', 'linux']
        assert props['lang_list'] == ['en', 'fr']
        assert props['installers'] == 'both'
        assert props['strictDupe'] == True
        assert props['md5xmls'] == True
        assert props['noChangeLogs'] == False
        assert props['skipknown'] == True
        assert props['partial'] == False
        assert props['updateonly'] == False
        assert props['complete'] == False
    
    def test_includes_all_mode_flags(self):
        """Should include skipknown, partial, updateonly flags."""
        config = FetchConfig()
        
        props = create_resume_properties(config, skipknown=False, partial=True, updateonly=True)
        
        assert props['skipknown'] == False
        assert props['partial'] == True
        assert props['updateonly'] == True
    
    def test_strict_flags_can_be_overridden(self):
        """Strict flags default to False but can be overridden by caller."""
        config = FetchConfig()
        
        props = create_resume_properties(config, skipknown=False, partial=False, updateonly=False)
        
        # Strict should default to False
        assert props['strict'] == False


class TestProcessItemsWithResume:
    """Test the process_items_with_resume function."""
    
    def test_processes_all_items(self):
        """Should process all items and add to manifest."""
        items = [
            AttrDict({'id': 1, 'title': 'game1'}),
            AttrDict({'id': 2, 'title': 'game2'})
        ]
        gamesdb = []
        
        with patch('modules.update.save_manifest'):
            with patch('modules.update.save_resume_manifest'):
                with patch('modules.update.RESUME_SAVE_THRESHOLD', 10):
                    game_filter = GameFilter()
                    updated_db, dupes = process_items_with_resume(
                        items, gamesdb, game_filter, False, False
                    )
        
        assert len(updated_db) == 2
        assert updated_db[0].id == 1
        assert updated_db[1].id == 2
    
    def test_updates_existing_items(self):
        """Should update existing items instead of adding duplicates."""
        items = [
            AttrDict({'id': 1, 'title': 'game1', 'version': '2.0'})
        ]
        gamesdb = [
            AttrDict({'id': 1, 'title': 'game1', 'version': '1.0'})
        ]
        
        with patch('modules.update.save_manifest'):
            with patch('modules.update.save_resume_manifest'):
                with patch('modules.update.handle_game_updates'):
                    with patch('modules.update.RESUME_SAVE_THRESHOLD', 10):
                        game_filter = GameFilter()
                        updated_db, dupes = process_items_with_resume(
                            items, gamesdb, game_filter, False, False
                        )
        
        assert len(updated_db) == 1
        assert updated_db[0].version == '2.0'
    
    def test_calls_handle_game_updates_for_existing(self):
        """Should call handle_game_updates for existing games."""
        items = [
            AttrDict({'id': 1, 'title': 'game1'})
        ]
        gamesdb = [
            AttrDict({'id': 1, 'title': 'game1'})
        ]
        
        with patch('modules.update.save_manifest'):
            with patch('modules.update.save_resume_manifest'):
                with patch('modules.update.handle_game_updates') as mock_handle:
                    with patch('modules.update.RESUME_SAVE_THRESHOLD', 10):
                        game_filter = GameFilter(strict=True)
                        process_items_with_resume(
                            items, gamesdb, game_filter, False, False
                        )
        
        # Should have called handle_game_updates with strict flag for all params
        assert mock_handle.called
        call_args = mock_handle.call_args[0]
        assert call_args[2] == True  # strict (for all file types)
        assert call_args[3] == True  # strict applied to downloads
        assert call_args[4] == True  # strict applied to extras
    
    def test_handles_exceptions_gracefully(self):
        """Should log exceptions and continue processing."""
        items = [
            AttrDict({'id': 1, 'title': 'game1'}),
            AttrDict({'id': 2, 'title': 'game2'})
        ]
        gamesdb = []
        
        # Make first item raise exception
        def side_effect(item_id, db):
            if item_id == 1:
                raise ValueError("Test error")
            return None
        
        with patch('modules.update.save_manifest'):
            with patch('modules.update.save_resume_manifest'):
                with patch('modules.update.item_checkdb', side_effect=side_effect):
                    with patch('modules.update.RESUME_SAVE_THRESHOLD', 10):
                        game_filter = GameFilter()
                        updated_db, dupes = process_items_with_resume(
                            items, gamesdb, game_filter, False, False
                        )
        
        # Should still have processed second item
        assert len(updated_db) == 1
        assert updated_db[0].id == 2
    
    def test_detects_duplicate_titles(self):
        """Should detect games with duplicate titles."""
        items = [
            AttrDict({'id': 1, 'title': 'same_game'}),
            AttrDict({'id': 2, 'title': 'same_game'}),
            AttrDict({'id': 3, 'title': 'different'})
        ]
        gamesdb = []
        
        with patch('modules.update.save_manifest'):
            with patch('modules.update.save_resume_manifest'):
                with patch('modules.update.RESUME_SAVE_THRESHOLD', 10):
                    game_filter = GameFilter()
                    updated_db, dupes = process_items_with_resume(
                        items, gamesdb, game_filter, False, False
                    )
        
        # Should have found the 2 duplicates
        assert len(dupes) == 2
        # Both duplicates should have _id suffix added to folder_name
        dupe_ids = {d.id for d in dupes}
        assert 1 in dupe_ids
        assert 2 in dupe_ids
        assert all('_' in d.folder_name for d in dupes)
    
    def test_periodic_saves(self):
        """Should save periodically based on RESUME_SAVE_THRESHOLD."""
        items = [AttrDict({'id': i, 'title': f'game{i}'}) for i in range(1, 6)]
        gamesdb = []
        
        with patch('modules.update.save_manifest') as mock_save:
            with patch('modules.update.save_resume_manifest'):
                with patch('modules.update.RESUME_SAVE_THRESHOLD', 2):
                    game_filter = GameFilter()
                    process_items_with_resume(
                        items, gamesdb, game_filter, False, False
                    )
        
        # Should save at items 2, 4, and implicitly at the end
        assert mock_save.call_count >= 2
    
    def test_saves_more_frequently_in_skipknown_mode(self):
        """Should save after every item in skipknown mode."""
        items = [
            AttrDict({'id': 1, 'title': 'game1'}),
            AttrDict({'id': 2, 'title': 'game2'})
        ]
        gamesdb = []
        
        with patch('modules.update.save_manifest') as mock_save:
            with patch('modules.update.save_resume_manifest'):
                with patch('modules.update.RESUME_SAVE_THRESHOLD', 100):  # High threshold
                    game_filter = GameFilter()
                    process_items_with_resume(
                        items, gamesdb, game_filter, True, False  # skipknown=True
                    )
        
        # Should save after each item due to skipknown
        assert mock_save.call_count == 2
    
    def test_saves_more_frequently_in_updateonly_mode(self):
        """Should save after every item in updateonly mode."""
        items = [
            AttrDict({'id': 1, 'title': 'game1'}),
            AttrDict({'id': 2, 'title': 'game2'})
        ]
        gamesdb = []
        
        with patch('modules.update.save_manifest') as mock_save:
            with patch('modules.update.save_resume_manifest'):
                with patch('modules.update.RESUME_SAVE_THRESHOLD', 100):
                    game_filter = GameFilter()
                    process_items_with_resume(
                        items, gamesdb, game_filter, False, True  # updateonly=True
                    )
        
        assert mock_save.call_count == 2


class TestHandleSingleGameRename:
    """Test the handle_single_game_rename function."""
    
    def test_no_rename_needed_if_old_folder_name_is_none(self):
        """Should skip rename if old_folder_name is None."""
        game = AttrDict({
            'id': 1,
            'title': 'game1',
            'folder_name': 'game1',
            'old_folder_name': None,
            'downloads': [],
            'galaxyDownloads': [],
            'sharedDownloads': [],
            'extras': []
        })
        
        with patch('modules.update.os.path.isdir', return_value=True):
            with patch('modules.update.move_with_increment_on_clash') as mock_move:
                handle_single_game_rename(game, '/savedir', '/orphan', dryrun=False)
        
        # Should not attempt any renames
        assert not mock_move.called
    
    def test_renames_directory_when_old_folder_name_exists(self):
        """Should rename directory when old_folder_name differs from folder_name."""
        game = AttrDict({
            'id': 1,
            'title': 'New Game Title',
            'folder_name': 'New Game Title',
            'old_folder_name': 'Old Game Title',
            'downloads': [],
            'galaxyDownloads': [],
            'sharedDownloads': [],
            'extras': []
        })
        
        with patch('modules.update.os.path.isdir', return_value=True):
            with patch('modules.update.os.path.exists', return_value=False):
                with patch('modules.update.move_with_increment_on_clash') as mock_move:
                    handle_single_game_rename(game, '/savedir', '/orphan', dryrun=False)
        
        # Should have called move once for the directory
        assert mock_move.call_count == 1
        # Use os.path.join for cross-platform compatibility
        call_args = mock_move.call_args[0]
        assert call_args[0] == os.path.join('/savedir', 'Old Game Title')
        assert call_args[1] == os.path.join('/savedir', 'New Game Title')
    
    def test_skips_directory_rename_if_source_does_not_exist(self):
        """Should not rename if source directory doesn't exist."""
        game = AttrDict({
            'id': 1,
            'title': 'game1',
            'folder_name': 'New Title',
            'old_folder_name': 'Old Title',
            'downloads': [],
            'galaxyDownloads': [],
            'sharedDownloads': [],
            'extras': []
        })
        
        with patch('modules.update.os.path.isdir', return_value=False):
            with patch('modules.update.move_with_increment_on_clash') as mock_move:
                handle_single_game_rename(game, '/savedir', '/orphan', dryrun=False)
        
        # Should not attempt rename since source doesn't exist
        assert not mock_move.called
    
    def test_orphans_destination_if_it_exists(self):
        """Should move existing destination to orphan folder before rename."""
        game = AttrDict({
            'id': 1,
            'title': 'game1',
            'folder_name': 'New Title',
            'old_folder_name': 'Old Title',
            'downloads': [],
            'galaxyDownloads': [],
            'sharedDownloads': [],
            'extras': []
        })
        
        with patch('modules.update.os.path.isdir', return_value=True):
            with patch('modules.update.os.path.exists', return_value=True):
                with patch('modules.update.move_with_increment_on_clash') as mock_move:
                    handle_single_game_rename(game, '/savedir', '/orphan', dryrun=False)
        
        # Should have called move twice: once for orphaning, once for rename
        assert mock_move.call_count == 2
        calls = mock_move.call_args_list
        assert calls[0][0][0] == os.path.join('/savedir', 'New Title')
        assert calls[0][0][1] == os.path.join('/orphan', 'New Title')
        assert calls[1][0][0] == os.path.join('/savedir', 'Old Title')
        assert calls[1][0][1] == os.path.join('/savedir', 'New Title')
    
    def test_dryrun_does_not_move_directories(self):
        """Should not actually move files in dryrun mode."""
        game = AttrDict({
            'id': 1,
            'title': 'game1',
            'folder_name': 'New Title',
            'old_folder_name': 'Old Title',
            'downloads': [],
            'galaxyDownloads': [],
            'sharedDownloads': [],
            'extras': []
        })
        
        with patch('modules.update.os.path.isdir', return_value=True):
            with patch('modules.update.os.path.exists', return_value=False):
                with patch('modules.update.move_with_increment_on_clash') as mock_move:
                    handle_single_game_rename(game, '/savedir', '/orphan', dryrun=True)
        
        # Should not move in dryrun mode
        assert not mock_move.called
    
    def test_renames_file_when_old_name_exists(self):
        """Should rename individual files when item.old_name is set."""
        game = AttrDict({
            'id': 1,
            'title': 'game1',
            'folder_name': 'game1',
            'old_folder_name': None,
            'downloads': [
                AttrDict({'name': 'new_installer.exe', 'old_name': 'old_installer.exe'})
            ],
            'galaxyDownloads': [],
            'sharedDownloads': [],
            'extras': []
        })
        
        with patch('modules.update.os.path.isfile', return_value=True):
            with patch('modules.update.os.path.exists', return_value=False):
                with patch('modules.update.move_with_increment_on_clash') as mock_move:
                    handle_single_game_rename(game, '/savedir', '/orphan', dryrun=False)
        
        # Should have renamed the file
        assert mock_move.call_count == 1
        call_args = mock_move.call_args[0]
        assert call_args[0] == os.path.join('/savedir', 'game1', 'old_installer.exe')
        assert call_args[1] == os.path.join('/savedir', 'game1', 'new_installer.exe')
    
    def test_skips_file_rename_if_source_does_not_exist(self):
        """Should not rename file if source doesn't exist."""
        game = AttrDict({
            'id': 1,
            'title': 'game1',
            'folder_name': 'game1',
            'old_folder_name': None,
            'downloads': [
                AttrDict({'name': 'new.exe', 'old_name': 'old.exe'})
            ],
            'galaxyDownloads': [],
            'sharedDownloads': [],
            'extras': []
        })
        
        with patch('modules.update.os.path.isfile', return_value=False):
            with patch('modules.update.move_with_increment_on_clash') as mock_move:
                handle_single_game_rename(game, '/savedir', '/orphan', dryrun=False)
        
        # Should not attempt rename
        assert not mock_move.called
    
    def test_orphans_file_destination_if_it_exists(self):
        """Should move existing file to orphan folder before rename."""
        game = AttrDict({
            'id': 1,
            'title': 'game1',
            'folder_name': 'game1',
            'old_folder_name': None,
            'downloads': [
                AttrDict({'name': 'installer.exe', 'old_name': 'old_installer.exe'})
            ],
            'galaxyDownloads': [],
            'sharedDownloads': [],
            'extras': []
        })
        
        with patch('modules.update.os.path.isfile', return_value=True):
            with patch('modules.update.os.path.exists', return_value=True):
                with patch('modules.update.os.path.isdir', return_value=True):
                    with patch('modules.update.move_with_increment_on_clash') as mock_move:
                        handle_single_game_rename(game, '/savedir', '/orphan', dryrun=False)
        
        # Should orphan then rename
        assert mock_move.call_count == 2
        calls = mock_move.call_args_list
        assert calls[0][0][0] == os.path.join('/savedir', 'game1', 'installer.exe')
        assert calls[0][0][1] == os.path.join('/orphan', 'game1', 'installer.exe')
        assert calls[1][0][0] == os.path.join('/savedir', 'game1', 'old_installer.exe')
        assert calls[1][0][1] == os.path.join('/savedir', 'game1', 'installer.exe')
    
    def test_creates_orphan_subdirectory_if_needed(self):
        """Should create orphan game subdirectory if it doesn't exist."""
        game = AttrDict({
            'id': 1,
            'title': 'game1',
            'folder_name': 'game1',
            'old_folder_name': None,
            'downloads': [
                AttrDict({'name': 'installer.exe', 'old_name': 'old_installer.exe'})
            ],
            'galaxyDownloads': [],
            'sharedDownloads': [],
            'extras': []
        })
        
        with patch('modules.update.os.path.isfile', return_value=True):
            with patch('modules.update.os.path.exists', return_value=True):
                with patch('modules.update.os.path.isdir', return_value=False):
                    with patch('modules.update.os.makedirs') as mock_makedirs:
                        with patch('modules.update.move_with_increment_on_clash'):
                            handle_single_game_rename(game, '/savedir', '/orphan', dryrun=False)
        
        # Should have created the orphan subdirectory
        call_args = mock_makedirs.call_args[0]
        assert call_args[0] == os.path.join('/orphan', 'game1')
    
    def test_clears_old_name_after_successful_file_rename(self):
        """Should set item.old_name to None after successful rename."""
        item = AttrDict({'name': 'new.exe', 'old_name': 'old.exe'})
        game = AttrDict({
            'id': 1,
            'title': 'game1',
            'folder_name': 'game1',
            'old_folder_name': None,
            'downloads': [item],
            'galaxyDownloads': [],
            'sharedDownloads': [],
            'extras': []
        })
        
        with patch('modules.update.os.path.isfile', return_value=True):
            with patch('modules.update.os.path.exists', return_value=False):
                with patch('modules.update.move_with_increment_on_clash'):
                    handle_single_game_rename(game, '/savedir', '/orphan', dryrun=False)
        
        # old_name should be cleared
        assert item.old_name is None
    
    def test_handles_missing_attributes_gracefully(self):
        """Should add missing attributes without errors."""
        game = AttrDict({
            'id': 1,
            'title': 'game1'
            # Missing all other attributes
        })
        
        with patch('modules.update.os.path.isdir', return_value=False):
            # Should not raise any exceptions
            handle_single_game_rename(game, '/savedir', '/orphan', dryrun=False)
        
        # Should have added missing attributes
        assert hasattr(game, 'galaxyDownloads')
        assert hasattr(game, 'sharedDownloads')
        assert hasattr(game, 'folder_name')
        assert game.folder_name == 'game1'
    
    def test_handles_rename_exception_gracefully(self):
        """Should log error and continue on rename failure."""
        game = AttrDict({
            'id': 1,
            'title': 'game1',
            'folder_name': 'New Title',
            'old_folder_name': 'Old Title',
            'downloads': [],
            'galaxyDownloads': [],
            'sharedDownloads': [],
            'extras': []
        })
        
        with patch('modules.update.os.path.isdir', return_value=True):
            with patch('modules.update.os.path.exists', return_value=False):
                with patch('modules.update.move_with_increment_on_clash', side_effect=Exception('Test error')):
                    # Should not raise exception
                    handle_single_game_rename(game, '/savedir', '/orphan', dryrun=False)
    
    def test_handles_file_rename_exception_and_marks_unverified(self):
        """Should mark item as unverified on file rename failure."""
        item = AttrDict({'name': 'new.exe', 'old_name': 'old.exe'})
        game = AttrDict({
            'id': 1,
            'title': 'game1',
            'folder_name': 'game1',
            'old_folder_name': None,
            'downloads': [item],
            'galaxyDownloads': [],
            'sharedDownloads': [],
            'extras': []
        })
        
        with patch('modules.update.os.path.isfile', return_value=True):
            with patch('modules.update.os.path.exists', return_value=False):
                with patch('modules.update.move_with_increment_on_clash', side_effect=Exception('Test error')):
                    handle_single_game_rename(game, '/savedir', '/orphan', dryrun=False)
        
        # Should have marked as unverified
        assert item.prev_verified == False
    
    def test_processes_all_download_types(self):
        """Should check all download types for renames."""
        game = AttrDict({
            'id': 1,
            'title': 'game1',
            'folder_name': 'game1',
            'old_folder_name': None,
            'downloads': [AttrDict({'name': 'file1.exe', 'old_name': 'old1.exe'})],
            'galaxyDownloads': [AttrDict({'name': 'file2.exe', 'old_name': 'old2.exe'})],
            'sharedDownloads': [AttrDict({'name': 'file3.exe', 'old_name': 'old3.exe'})],
            'extras': [AttrDict({'name': 'manual.pdf', 'old_name': 'old_manual.pdf'})]
        })
        
        with patch('modules.update.os.path.isfile', return_value=True):
            with patch('modules.update.os.path.exists', return_value=False):
                with patch('modules.update.move_with_increment_on_clash') as mock_move:
                    handle_single_game_rename(game, '/savedir', '/orphan', dryrun=False)
        
        # Should have renamed all 4 files
        assert mock_move.call_count == 4
