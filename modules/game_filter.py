#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Game filtering logic for GOG game selection.

Provides centralized filtering logic used across multiple commands (update, download,
import, backup, verify) to determine which games should be processed based on various
criteria like game IDs, update status, and visibility.
"""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class GameFilter:
    """Encapsulates game filtering parameters.
    
    This object is passed to filtering functions to determine which games should
    be processed. Supports filtering by ID lists, update status, and visibility.
    
    Attributes:
        ids: List of game titles or IDs to include. If provided, ONLY games
            matching these IDs will be processed. Supports both slugs 
            (e.g., 'witcher_3') and numeric IDs. Empty list means include all.
        skipids: List of game titles or IDs to exclude. Games matching these
            IDs will never be processed. Empty list means no exclusions.
        skipknown: If True, only process games NOT already in manifest.
            Used for incremental updates. Requires known_ids to be set.
        updateonly: If True, only process games that GOG reports as having updates.
            Requires game.has_updates attribute to be present.
        skipHidden: If True, exclude games marked as hidden in GOG library.
            Requires game to have appropriate visibility flag.
        known_ids: List of game IDs already in the manifest. Used with skipknown
            to determine which games are "new".
        strict: If True, force thorough timestamp/MD5 checking for all file types.
            At selection: Include games even without has_updates flag.
            At checking: Force timestamp checks and mark changed files for re-download.
        os_list: List of operating systems to include (e.g., ['windows', 'linux']).
            Empty list means no OS filtering.
        lang_list: List of languages to include (e.g., ['en', 'fr']).
            Empty list means no language filtering.
        installers: Installer type filter - 'standalone', 'galaxy', 'shared', or 'all'.
            Controls which download types are included.
        skip_extras: If True, skip extra content (soundtracks, manuals, etc.).
    
    Examples:
        >>> # Include only specific games
        >>> filter = GameFilter(ids=['witcher_3', '1234567'])
        
        >>> # Exclude specific games
        >>> filter = GameFilter(skipids=['problematic_game'])
        
        >>> # Only new games not in manifest
        >>> filter = GameFilter(skipknown=True, known_ids=[123, 456, 789])
        
        >>> # Only games with updates
        >>> filter = GameFilter(updateonly=True)
        
        >>> # Windows games only, skip extras
        >>> filter = GameFilter(os_list=['windows'], skip_extras=True)
    """
    ids: List[str] = field(default_factory=list)
    skipids: List[str] = field(default_factory=list)
    skipknown: bool = False
    updateonly: bool = False
    skipHidden: bool = False
    known_ids: List[int] = field(default_factory=list)
    strict: bool = False
    os_list: List[str] = field(default_factory=list)
    lang_list: List[str] = field(default_factory=list)
    installers: str = "all"
    skip_extras: bool = False


def game_matches_id(game, id_value):
    """Check if a game matches a given ID value (title or numeric ID).
    
    Supports matching by game title (slug) or numeric game ID. Handles
    both string and integer ID values.
    
    Args:
        game: AttrDict with 'id' and 'title' attributes
        id_value: String or int to match against (can be title or game ID)
        
    Returns:
        bool: True if game matches the ID value
        
    Examples:
        >>> game = AttrDict({'id': 1234567, 'title': 'witcher_3'})
        >>> game_matches_id(game, 'witcher_3')
        True
        >>> game_matches_id(game, '1234567')
        True
        >>> game_matches_id(game, 1234567)
        True
        >>> game_matches_id(game, 'other_game')
        False
    """
    return game.title == id_value or str(game.id) == str(id_value)


def should_process_game_by_id(game, filter_obj):
    """Determine if game should be processed based on ID filters only.
    
    This is the foundational filter that checks include (ids) and exclude (skipids)
    lists. Other filters build upon this.
    
    Processing logic:
    1. If game is in skipids → return False (exclusion takes priority)
    2. If ids list is provided and game NOT in it → return False
    3. Otherwise → return True
    
    Args:
        game: AttrDict with 'id' and 'title' attributes
        filter_obj: GameFilter object containing filtering parameters
        
    Returns:
        bool: True if game should be processed based on ID filters
        
    Examples:
        >>> game = AttrDict({'id': 123, 'title': 'game_a'})
        >>> filter = GameFilter(skipids=['game_a'])
        >>> should_process_game_by_id(game, filter)
        False
        
        >>> filter = GameFilter(ids=['game_a', 'game_b'])
        >>> should_process_game_by_id(game, filter)
        True
        
        >>> filter = GameFilter(ids=['game_b'])
        >>> should_process_game_by_id(game, filter)
        False
    """
    # Exclusion takes priority - check skipids first
    if filter_obj.skipids:
        if game_matches_id(game, filter_obj.skipids[0]):
            return False
        for skip_id in filter_obj.skipids:
            if game_matches_id(game, skip_id):
                return False
    
    # If specific IDs requested, only include those
    if filter_obj.ids:
        for include_id in filter_obj.ids:
            if game_matches_id(game, include_id):
                return True
        return False  # Not in include list
    
    # No filters means include everything
    return True


def should_process_game_by_update_status(game, filter_obj):
    """Determine if game should be processed based on update status filters.
    
    Handles skipknown and updateonly filters to determine if a game should
    be processed based on whether it's new or has updates.
    
    Processing logic:
    1. If skipknown=True → only include games NOT in known_ids (new games)
    2. If updateonly=True → only include games with has_updates=True
    3. If both are False → include all games (no update status filtering)
    
    Note: This function assumes the game has already passed ID filtering.
    In partial mode (skipknown AND updateonly both True), games are included
    if they meet EITHER criterion (new OR updated).
    
    Args:
        game: AttrDict with 'id' attribute and optionally 'has_updates' attribute
        filter_obj: GameFilter object with skipknown, updateonly, and known_ids
        
    Returns:
        bool: True if game should be processed based on update status
        
    Examples:
        >>> game = AttrDict({'id': 999, 'has_updates': False})
        >>> filter = GameFilter(skipknown=True, known_ids=[123, 456])
        >>> should_process_game_by_update_status(game, filter)
        True  # New game not in known_ids
        
        >>> game = AttrDict({'id': 123, 'has_updates': True})
        >>> filter = GameFilter(updateonly=True)
        >>> should_process_game_by_update_status(game, filter)
        True  # Game has updates
        
        >>> game = AttrDict({'id': 123, 'has_updates': False})
        >>> filter = GameFilter(updateonly=True)
        >>> should_process_game_by_update_status(game, filter)
        False  # Game has no updates
    """
    # If no update status filters, include everything
    if not filter_obj.skipknown and not filter_obj.updateonly:
        return True
    
    # Check skipknown: is this a new game?
    is_new_game = False
    if filter_obj.skipknown:
        is_new_game = game.id not in filter_obj.known_ids
    
    # Check updateonly: does this game have updates?
    has_updates = False
    if filter_obj.updateonly:
        has_updates = getattr(game, 'has_updates', False)
    
    # In partial mode (both filters), include if EITHER condition is true
    # Otherwise, include only if the specific condition is true
    if filter_obj.skipknown and filter_obj.updateonly:
        return is_new_game or has_updates
    elif filter_obj.skipknown:
        return is_new_game
    elif filter_obj.updateonly:
        return has_updates
    
    return True


def should_process_game_by_visibility(game, filter_obj):
    """Determine if game should be processed based on visibility filters.
    
    Handles skipHidden filter to exclude games marked as hidden in GOG library.
    
    Args:
        game: AttrDict that may have 'isHidden' attribute
        filter_obj: GameFilter object with skipHidden flag
        
    Returns:
        bool: True if game should be processed based on visibility
        
    Examples:
        >>> game = AttrDict({'id': 123, 'isHidden': True})
        >>> filter = GameFilter(skipHidden=True)
        >>> should_process_game_by_visibility(game, filter)
        False  # Hidden game is excluded
        
        >>> game = AttrDict({'id': 123, 'isHidden': False})
        >>> filter = GameFilter(skipHidden=True)
        >>> should_process_game_by_visibility(game, filter)
        True  # Visible game is included
    """
    if not filter_obj.skipHidden:
        return True
    
    # If skipHidden is True, exclude games with isHidden=True
    is_hidden = getattr(game, 'isHidden', False)
    return not is_hidden


def should_process_game(game, filter_obj):
    """Master filter function combining all game filtering criteria.
    
    Applies all filtering logic in order:
    1. ID filters (ids/skipids)
    2. Update status filters (skipknown/updateonly)
    3. Visibility filters (skipHidden)
    
    All filters must pass for a game to be included. This is the main
    entry point for game filtering that should be used by commands.
    
    Args:
        game: AttrDict with game data (requires 'id' and 'title', may have
            'has_updates' and 'isHidden' depending on which filters are active)
        filter_obj: GameFilter object containing all filtering parameters
        
    Returns:
        bool: True if game passes all filters and should be processed
        
    Examples:
        >>> game = AttrDict({'id': 123, 'title': 'wanted', 'has_updates': True})
        >>> filter = GameFilter(ids=['wanted'], updateonly=True)
        >>> should_process_game(game, filter)
        True
        
        >>> game = AttrDict({'id': 123, 'title': 'hidden', 'isHidden': True})
        >>> filter = GameFilter(skipHidden=True)
        >>> should_process_game(game, filter)
        False
    """
    # Apply filters in order - all must pass
    if not should_process_game_by_id(game, filter_obj):
        return False
    
    if not should_process_game_by_update_status(game, filter_obj):
        return False
    
    if not should_process_game_by_visibility(game, filter_obj):
        return False
    
    return True


def filter_game_list(games, filter_obj):
    """Filter a list of games using the provided filter object.
    
    Convenience function that applies should_process_game() to a list of games
    and returns only those that pass all filters.
    
    Args:
        games: List of game AttrDicts
        filter_obj: GameFilter object containing filtering parameters
        
    Returns:
        List of games that pass all filters
        
    Examples:
        >>> games = [
        ...     AttrDict({'id': 1, 'title': 'game_a'}),
        ...     AttrDict({'id': 2, 'title': 'game_b'}),
        ...     AttrDict({'id': 3, 'title': 'game_c'})
        ... ]
        >>> filter = GameFilter(ids=['game_a', 'game_c'])
        >>> filtered = filter_game_list(games, filter)
        >>> len(filtered)
        2
    """
    return [game for game in games if should_process_game(game, filter_obj)]


def create_filter_for_full_update(known_ids=None):
    """Create a GameFilter for full library updates (no game filtering).
    
    Returns an empty filter that will include all games. This is a convenience
    function to make it explicit when no filtering is desired.
    
    Args:
        known_ids: Optional list of known game IDs for tracking purposes
        
    Returns:
        GameFilter with no filters active (includes all games)
        
    Example:
        >>> filter = create_filter_for_full_update()
        >>> all_games = filter_game_list(games, filter)  # Returns all games
    """
    return GameFilter(known_ids=known_ids or [])


def create_filter_for_specific_games(game_ids, skipids=None, known_ids=None):
    """Create a GameFilter for specific game IDs.
    
    Convenience function to create a filter that includes only specified games
    and optionally excludes others.
    
    Args:
        game_ids: List of game titles or IDs to include
        skipids: Optional list of game titles or IDs to exclude
        known_ids: Optional list of known game IDs for tracking purposes
        
    Returns:
        GameFilter configured for specific game selection
        
    Example:
        >>> filter = create_filter_for_specific_games(['witcher_3', 'cyberpunk'])
        >>> filtered = filter_game_list(games, filter)
    """
    return GameFilter(
        ids=game_ids,
        skipids=skipids or [],
        known_ids=known_ids or []
    )
