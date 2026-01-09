"""
Tests for game filtering logic.

Tests the game_filter module which provides centralized game selection
logic used across multiple commands.
"""
import pytest
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from modules.game_filter import (
    GameFilter, game_matches_id, should_process_game_by_id,
    should_process_game_by_update_status, should_process_game_by_visibility,
    should_process_game, filter_game_list,
    create_filter_for_full_update, create_filter_for_specific_games
)
from modules.utils import AttrDict


class TestGameMatchesId:
    """Test the game_matches_id helper function."""
    
    def test_matches_by_title(self):
        """Game should match by title string."""
        game = AttrDict({'id': 1234567, 'title': 'witcher_3'})
        assert game_matches_id(game, 'witcher_3') == True
    
    def test_matches_by_numeric_id(self):
        """Game should match by numeric ID."""
        game = AttrDict({'id': 1234567, 'title': 'witcher_3'})
        assert game_matches_id(game, 1234567) == True
    
    def test_matches_by_string_id(self):
        """Game should match by string representation of ID."""
        game = AttrDict({'id': 1234567, 'title': 'witcher_3'})
        assert game_matches_id(game, '1234567') == True
    
    def test_does_not_match_different_title(self):
        """Game should not match different title."""
        game = AttrDict({'id': 1234567, 'title': 'witcher_3'})
        assert game_matches_id(game, 'cyberpunk') == False
    
    def test_does_not_match_different_id(self):
        """Game should not match different ID."""
        game = AttrDict({'id': 1234567, 'title': 'witcher_3'})
        assert game_matches_id(game, 9999999) == False


class TestShouldProcessGameById:
    """Test ID-based filtering (ids and skipids)."""
    
    def test_no_filters_includes_all_games(self):
        """With no filters, all games should be included."""
        game = AttrDict({'id': 123, 'title': 'any_game'})
        filter = GameFilter()
        
        assert should_process_game_by_id(game, filter) == True
    
    def test_skipids_excludes_by_title(self):
        """Games in skipids list should be excluded (by title)."""
        game = AttrDict({'id': 123, 'title': 'exclude_me'})
        filter = GameFilter(skipids=['exclude_me'])
        
        assert should_process_game_by_id(game, filter) == False
    
    def test_skipids_excludes_by_id(self):
        """Games in skipids list should be excluded (by numeric ID)."""
        game = AttrDict({'id': 123, 'title': 'some_game'})
        filter = GameFilter(skipids=['123'])
        
        assert should_process_game_by_id(game, filter) == False
    
    def test_skipids_allows_non_excluded_games(self):
        """Games NOT in skipids should be included."""
        game = AttrDict({'id': 123, 'title': 'include_me'})
        filter = GameFilter(skipids=['other_game'])
        
        assert should_process_game_by_id(game, filter) == True
    
    def test_ids_includes_specified_game_by_title(self):
        """When ids provided, game in list should be included (by title)."""
        game = AttrDict({'id': 123, 'title': 'wanted_game'})
        filter = GameFilter(ids=['wanted_game', 'other_game'])
        
        assert should_process_game_by_id(game, filter) == True
    
    def test_ids_includes_specified_game_by_id(self):
        """When ids provided, game in list should be included (by ID)."""
        game = AttrDict({'id': 123, 'title': 'some_game'})
        filter = GameFilter(ids=['123', '456'])
        
        assert should_process_game_by_id(game, filter) == True
    
    def test_ids_excludes_unspecified_game(self):
        """When ids provided, game NOT in list should be excluded."""
        game = AttrDict({'id': 999, 'title': 'unwanted_game'})
        filter = GameFilter(ids=['wanted_game', '123'])
        
        assert should_process_game_by_id(game, filter) == False
    
    def test_skipids_takes_priority_over_ids(self):
        """Exclusion (skipids) should take priority over inclusion (ids)."""
        game = AttrDict({'id': 123, 'title': 'conflicted_game'})
        filter = GameFilter(
            ids=['conflicted_game'],
            skipids=['conflicted_game']
        )
        
        assert should_process_game_by_id(game, filter) == False
    
    def test_multiple_skipids(self):
        """Multiple games can be excluded."""
        game1 = AttrDict({'id': 123, 'title': 'skip_a'})
        game2 = AttrDict({'id': 456, 'title': 'skip_b'})
        game3 = AttrDict({'id': 789, 'title': 'include_me'})
        filter = GameFilter(skipids=['skip_a', 'skip_b'])
        
        assert should_process_game_by_id(game1, filter) == False
        assert should_process_game_by_id(game2, filter) == False
        assert should_process_game_by_id(game3, filter) == True
    
    def test_multiple_ids(self):
        """Multiple specific games can be included."""
        game1 = AttrDict({'id': 123, 'title': 'include_a'})
        game2 = AttrDict({'id': 456, 'title': 'include_b'})
        game3 = AttrDict({'id': 789, 'title': 'exclude_me'})
        filter = GameFilter(ids=['include_a', 'include_b'])
        
        assert should_process_game_by_id(game1, filter) == True
        assert should_process_game_by_id(game2, filter) == True
        assert should_process_game_by_id(game3, filter) == False


class TestGameFilterDataclass:
    """Test the GameFilter dataclass."""
    
    def test_default_values(self):
        """GameFilter should have sensible defaults."""
        filter = GameFilter()
        
        assert filter.ids == []
        assert filter.skipids == []
        assert filter.skipknown == False
        assert filter.updateonly == False
        assert filter.skipHidden == False
        assert filter.known_ids == []
    
    def test_can_set_ids(self):
        """Can initialize GameFilter with ids."""
        filter = GameFilter(ids=['game1', 'game2'])
        
        assert filter.ids == ['game1', 'game2']
    
    def test_can_set_skipids(self):
        """Can initialize GameFilter with skipids."""
        filter = GameFilter(skipids=['bad_game'])
        
        assert filter.skipids == ['bad_game']
    
    def test_can_modify_after_creation(self):
        """GameFilter fields can be modified after creation."""
        filter = GameFilter()
        filter.ids = ['new_game']
        
        assert filter.ids == ['new_game']


class TestShouldProcessGameByUpdateStatus:
    """Test update status filtering (skipknown and updateonly)."""
    
    def test_no_filters_includes_all_games(self):
        """With no update filters, all games should be included."""
        game = AttrDict({'id': 123, 'has_updates': False})
        filter = GameFilter()
        
        assert should_process_game_by_update_status(game, filter) == True
    
    def test_skipknown_includes_new_game(self):
        """skipknown should include games not in known_ids."""
        game = AttrDict({'id': 999, 'has_updates': False})
        filter = GameFilter(skipknown=True, known_ids=[123, 456, 789])
        
        assert should_process_game_by_update_status(game, filter) == True
    
    def test_skipknown_excludes_known_game(self):
        """skipknown should exclude games already in known_ids."""
        game = AttrDict({'id': 123, 'has_updates': False})
        filter = GameFilter(skipknown=True, known_ids=[123, 456, 789])
        
        assert should_process_game_by_update_status(game, filter) == False
    
    def test_updateonly_includes_game_with_updates(self):
        """updateonly should include games with has_updates=True."""
        game = AttrDict({'id': 123, 'has_updates': True})
        filter = GameFilter(updateonly=True)
        
        assert should_process_game_by_update_status(game, filter) == True
    
    def test_updateonly_excludes_game_without_updates(self):
        """updateonly should exclude games with has_updates=False."""
        game = AttrDict({'id': 123, 'has_updates': False})
        filter = GameFilter(updateonly=True)
        
        assert should_process_game_by_update_status(game, filter) == False
    
    def test_updateonly_handles_missing_has_updates(self):
        """updateonly should treat missing has_updates as False."""
        game = AttrDict({'id': 123})  # No has_updates attribute
        filter = GameFilter(updateonly=True)
        
        assert should_process_game_by_update_status(game, filter) == False
    
    def test_partial_mode_includes_new_game(self):
        """Partial mode (skipknown+updateonly) should include new games."""
        game = AttrDict({'id': 999, 'has_updates': False})
        filter = GameFilter(skipknown=True, updateonly=True, known_ids=[123, 456])
        
        assert should_process_game_by_update_status(game, filter) == True
    
    def test_partial_mode_includes_updated_game(self):
        """Partial mode should include known games with updates."""
        game = AttrDict({'id': 123, 'has_updates': True})
        filter = GameFilter(skipknown=True, updateonly=True, known_ids=[123, 456])
        
        assert should_process_game_by_update_status(game, filter) == True
    
    def test_partial_mode_excludes_known_game_without_updates(self):
        """Partial mode should exclude known games without updates."""
        game = AttrDict({'id': 123, 'has_updates': False})
        filter = GameFilter(skipknown=True, updateonly=True, known_ids=[123, 456])
        
        assert should_process_game_by_update_status(game, filter) == False


class TestShouldProcessGameByVisibility:
    """Test visibility filtering (skipHidden)."""
    
    def test_no_filter_includes_all_games(self):
        """With skipHidden=False, all games should be included."""
        hidden_game = AttrDict({'id': 123, 'isHidden': True})
        visible_game = AttrDict({'id': 456, 'isHidden': False})
        filter = GameFilter(skipHidden=False)
        
        assert should_process_game_by_visibility(hidden_game, filter) == True
        assert should_process_game_by_visibility(visible_game, filter) == True
    
    def test_skiphidden_excludes_hidden_game(self):
        """skipHidden should exclude games with isHidden=True."""
        game = AttrDict({'id': 123, 'isHidden': True})
        filter = GameFilter(skipHidden=True)
        
        assert should_process_game_by_visibility(game, filter) == False
    
    def test_skiphidden_includes_visible_game(self):
        """skipHidden should include games with isHidden=False."""
        game = AttrDict({'id': 123, 'isHidden': False})
        filter = GameFilter(skipHidden=True)
        
        assert should_process_game_by_visibility(game, filter) == True
    
    def test_skiphidden_handles_missing_attribute(self):
        """skipHidden should treat missing isHidden as False (visible)."""
        game = AttrDict({'id': 123})  # No isHidden attribute
        filter = GameFilter(skipHidden=True)
        
        assert should_process_game_by_visibility(game, filter) == True


class TestShouldProcessGame:
    """Test master filter combining all criteria."""
    
    def test_passes_all_filters(self):
        """Game passing all filters should be included."""
        game = AttrDict({
            'id': 999,
            'title': 'new_game',
            'has_updates': False,
            'isHidden': False
        })
        filter = GameFilter(
            ids=['new_game', 'other_game'],
            skipknown=True,
            known_ids=[123, 456],
            skipHidden=True
        )
        
        assert should_process_game(game, filter) == True
    
    def test_fails_id_filter(self):
        """Game failing ID filter should be excluded."""
        game = AttrDict({'id': 123, 'title': 'excluded_game'})
        filter = GameFilter(skipids=['excluded_game'])
        
        assert should_process_game(game, filter) == False
    
    def test_fails_update_status_filter(self):
        """Game failing update status filter should be excluded."""
        game = AttrDict({'id': 123, 'has_updates': False})
        filter = GameFilter(updateonly=True)
        
        assert should_process_game(game, filter) == False
    
    def test_fails_visibility_filter(self):
        """Game failing visibility filter should be excluded."""
        game = AttrDict({'id': 123, 'isHidden': True})
        filter = GameFilter(skipHidden=True)
        
        assert should_process_game(game, filter) == False
    
    def test_complex_partial_mode_scenario(self):
        """Test realistic partial mode filtering."""
        games = [
            AttrDict({'id': 1, 'title': 'existing_unchanged', 'has_updates': False}),
            AttrDict({'id': 2, 'title': 'existing_updated', 'has_updates': True}),
            AttrDict({'id': 999, 'title': 'new_game', 'has_updates': False}),
        ]
        filter = GameFilter(
            skipknown=True,
            updateonly=True,
            known_ids=[1, 2]
        )
        
        # Existing unchanged game should be excluded
        assert should_process_game(games[0], filter) == False
        # Existing updated game should be included
        assert should_process_game(games[1], filter) == True
        # New game should be included
        assert should_process_game(games[2], filter) == True


class TestFilterGameList:
    """Test the convenience list filtering function."""
    
    def test_filters_list_by_ids(self):
        """Should filter list to only specified IDs."""
        games = [
            AttrDict({'id': 1, 'title': 'game_a'}),
            AttrDict({'id': 2, 'title': 'game_b'}),
            AttrDict({'id': 3, 'title': 'game_c'}),
        ]
        filter = GameFilter(ids=['game_a', 'game_c'])
        
        filtered = filter_game_list(games, filter)
        
        assert len(filtered) == 2
        assert filtered[0].title == 'game_a'
        assert filtered[1].title == 'game_c'
    
    def test_filters_list_by_skipids(self):
        """Should filter out excluded IDs."""
        games = [
            AttrDict({'id': 1, 'title': 'game_a'}),
            AttrDict({'id': 2, 'title': 'game_b'}),
            AttrDict({'id': 3, 'title': 'game_c'}),
        ]
        filter = GameFilter(skipids=['game_b'])
        
        filtered = filter_game_list(games, filter)
        
        assert len(filtered) == 2
        assert all(g.title != 'game_b' for g in filtered)
    
    def test_filters_empty_list(self):
        """Should handle empty game list."""
        games = []
        filter = GameFilter(ids=['anything'])
        
        filtered = filter_game_list(games, filter)
        
        assert filtered == []
    
    def test_no_filters_returns_all(self):
        """With no filters, should return all games."""
        games = [
            AttrDict({'id': 1, 'title': 'game_a'}),
            AttrDict({'id': 2, 'title': 'game_b'}),
        ]
        filter = GameFilter()
        
        filtered = filter_game_list(games, filter)
        
        assert len(filtered) == 2


class TestFilterConvenienceFunctions:
    """Test convenience functions for creating filters."""
    
    def test_create_filter_for_full_update(self):
        """Should create empty filter for full updates."""
        filter = create_filter_for_full_update()
        
        assert filter.ids == []
        assert filter.skipids == []
        assert filter.skipknown == False
        assert filter.updateonly == False
    
    def test_create_filter_for_full_update_with_known_ids(self):
        """Should accept known_ids for tracking."""
        filter = create_filter_for_full_update(known_ids=[123, 456])
        
        assert filter.known_ids == [123, 456]
    
    def test_full_update_filter_includes_all_games(self):
        """Filter from create_filter_for_full_update should include all games."""
        games = [
            AttrDict({'id': 1, 'title': 'game_a'}),
            AttrDict({'id': 2, 'title': 'game_b'}),
            AttrDict({'id': 3, 'title': 'game_c'}),
        ]
        filter = create_filter_for_full_update()
        
        filtered = filter_game_list(games, filter)
        
        assert len(filtered) == 3
    
    def test_create_filter_for_specific_games(self):
        """Should create filter with specified game IDs."""
        filter = create_filter_for_specific_games(['game_a', 'game_b'])
        
        assert filter.ids == ['game_a', 'game_b']
        assert filter.skipids == []
    
    def test_create_filter_for_specific_games_with_skipids(self):
        """Should accept skipids parameter."""
        filter = create_filter_for_specific_games(
            ['game_a', 'game_b'], 
            skipids=['game_c']
        )
        
        assert filter.ids == ['game_a', 'game_b']
        assert filter.skipids == ['game_c']
    
    def test_specific_games_filter_works(self):
        """Filter from create_filter_for_specific_games should filter correctly."""
        games = [
            AttrDict({'id': 1, 'title': 'game_a'}),
            AttrDict({'id': 2, 'title': 'game_b'}),
            AttrDict({'id': 3, 'title': 'game_c'}),
        ]
        filter = create_filter_for_specific_games(['game_a', 'game_c'])
        
        filtered = filter_game_list(games, filter)
        
        assert len(filtered) == 2
        assert filtered[0].title == 'game_a'
        assert filtered[1].title == 'game_c'
