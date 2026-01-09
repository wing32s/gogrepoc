#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Game manifest update logic for GOG library.

Provides functions to fetch game data from GOG API and update the local manifest.
Organized by update strategies (full library, specific games, partial updates, etc).

Update Strategies:
    - update_full_library(): Fetch and update ALL games (no filtering)
    - update_specific_games(): Update only games with specified IDs
    - update_partial(): Update new games OR games with updates (efficient routine maintenance)
    - update_new_games_only(): Add only new games, ignore existing games
    - update_changed_games_only(): Update only games GOG reports as having changes

All strategies use the game_filter module for consistent game selection logic.
"""

import sys
import os
from dataclasses import dataclass
from typing import Optional, List, Tuple

from .utils import AttrDict, info, warn, error, debug, GOG_ACCOUNT_URL, GOG_MEDIA_TYPE_GAME, RESUME_MANIFEST_SYNTAX_VERSION, RESUME_SAVE_THRESHOLD, ORPHAN_DIR_NAME, log_exception
from .api import request
from .manifest import (
    filter_downloads, filter_extras, filter_dlcs, deDuplicateList,
    load_resume_manifest, save_resume_manifest, save_manifest, item_checkdb,
    handle_game_updates, move_with_increment_on_clash
)
from .game_filter import (
    GameFilter, should_process_game, filter_game_list,
    create_filter_for_full_update, create_filter_for_specific_games
)


@dataclass
class FetchConfig:
    """Configuration for fetching and filtering game data from GOG API.
    
    Attributes:
        os_list: List of OS types to include downloads for (e.g., ['windows', 'linux'])
        lang_list: List of language codes to include downloads for (e.g., ['en', 'de'])
        installers: Installer type filter - 'both', 'standalone', or 'galaxy'
        strict_dupe: Whether to apply strict deduplication to downloads
        md5xmls: Whether to fetch MD5 checksum XML files
        no_changelogs: Whether to exclude changelog data
    """
    os_list: Optional[List[str]] = None
    lang_list: Optional[List[str]] = None
    installers: str = 'both'
    strict_dupe: bool = True
    md5xmls: bool = True
    no_changelogs: bool = False


def create_filter_for_partial_update(known_ids):
    """Create a GameFilter for partial updates (new OR updated games).
    
    Args:
        known_ids: List of game IDs already in manifest
        
    Returns:
        GameFilter configured for partial mode
    """
    return GameFilter(
        skipknown=True,
        updateonly=True,
        known_ids=known_ids
    )


def create_filter_for_new_games(known_ids):
    """Create a GameFilter for new games only.
    
    Args:
        known_ids: List of game IDs already in manifest
        
    Returns:
        GameFilter configured to only include new games
    """
    return GameFilter(
        skipknown=True,
        known_ids=known_ids
    )


def create_filter_for_updated_games():
    """Create a GameFilter for games with updates only.
    
    Returns:
        GameFilter configured to only include games with has_updates=True
    """
    return GameFilter(updateonly=True)


def fetch_all_product_ids(session):
    """Fetch list of all game IDs from GOG product API.
    
    Paginates through GOG's getFilteredProducts API to retrieve basic information
    about all games in the user's library. Returns minimal game info (id, title,
    has_updates flag) without detailed download information.
    
    Args:
        session: Authenticated GOG session object
        
    Returns:
        List of AttrDict objects, each containing:
            - id: Game ID
            - title: Game slug/title
            - long_title: Full game title
            - has_updates: Boolean indicating if game has updates
            - Other basic metadata (genre, rating, store_url, etc.)
            
    Raises:
        SystemExit: If API returns invalid data or authentication fails
        
    Notes:
        - Handles pagination automatically
        - Sorts results by title
        - Skips hidden games if configured
    """
    products = []
    page = 0
    api_url = GOG_ACCOUNT_URL + "/getFilteredProducts"
    
    while True:
        page += 1
        if page == 1:
            info('fetching game product data (page %d)...' % page)
        else:
            info('fetching game product data (page %d / %d)...' % (page, json_data['totalPages']))
        
        response = request(session, api_url, args={
            'mediaType': GOG_MEDIA_TYPE_GAME,
            'sortBy': 'title',
            'page': str(page)
        })
        
        try:
            json_data = response.json()
        except ValueError:
            error('failed to load product data (are you still logged in?)')
            raise SystemExit(1)
        
        # Parse each product in the page
        for item_json_data in json_data['products']:
            item = AttrDict()
            item.id = item_json_data['id']
            item.title = item_json_data['slug']
            item.folder_name = item_json_data['slug']
            item.long_title = item_json_data['title']
            item.genre = item_json_data['category']
            item.image_url = item_json_data['image']
            item.store_url = item_json_data['url']
            item.media_type = GOG_MEDIA_TYPE_GAME
            item.rating = item_json_data['rating']
            item.has_updates = bool(item_json_data['updates'])
            item.old_title = None
            
            # Mirror key fields at top of structure for readability
            item._title_mirror = item.title
            item._long_title_mirror = item.long_title
            item._id_mirror = item.id
            
            # Store extra GOG data
            item.gog_data = AttrDict()
            for key in item_json_data:
                try:
                    tmp_contents = item[key]
                    if tmp_contents != item_json_data[key]:
                        debug("GOG Data Key, %s, for item clashes with Item Data Key storing detailed info in secondary dict" % key)
                        item.gog_data[key] = item_json_data[key]
                except Exception:
                    item[key] = item_json_data[key]
            
            products.append(item)
        
        # Check if we've reached the last page
        if page >= json_data['totalPages']:
            break
    
    return products


def fetch_and_merge_manifest(session, filtered_products, config):
    """Fetch detailed data for multiple games and merge with product metadata.
    
    This is a helper function used by all update strategies to avoid code duplication.
    Takes a filtered list of products and fetches detailed data for each one,
    merging the product metadata with the detailed game data.
    
    Args:
        session: Authenticated GOG session object
        filtered_products: List of product AttrDicts from fetch_all_product_ids()
        config: FetchConfig object containing download and filter settings
        
    Returns:
        List of complete game AttrDicts ready for manifest
    """
    manifest = []
    
    for i, product in enumerate(filtered_products, 1):
        info("(%d / %d)" % (i, len(filtered_products)))
        
        game_data = fetch_and_parse_game_details(
            session,
            product.id,
            product.title,
            config
        )
        
        if game_data:
            # Merge product list data with detailed data
            for key in product:
                if key not in game_data:
                    game_data[key] = product[key]
            manifest.append(game_data)
    
    info('successfully updated %d game(s)' % len(manifest))
    return manifest


def check_resume_needed(resumemode):
    """Check if there's an incomplete update to resume.
    
    Loads the resume manifest and determines if resumption is needed based on
    the resume mode and completion status.
    
    Args:
        resumemode: Resume behavior ('resume', 'noresume', 'onlyresume')
        
    Returns:
        Tuple of (needresume: bool, resumedb: list or None, resumeprops: dict or None)
        - needresume: True if update should be resumed
        - resumedb: List of remaining items to process (None if not resuming)
        - resumeprops: Dict of saved parameters from interrupted run (None if not resuming)
        
    Notes:
        - Prompts user if resume manifest has incompatible version
        - Returns (False, None, None) if no resume needed or resume discarded
    """
    try:
        resumedb = load_resume_manifest()
        resumeprops = resumedb.pop()
        needresume = resumemode != "noresume" and not resumeprops['complete']
        
        try:
            resume_manifest_syntax_version = resumeprops['resume_manifest_syntax_version']
        except KeyError:
            resume_manifest_syntax_version = -1
            
        if resume_manifest_syntax_version != RESUME_MANIFEST_SYNTAX_VERSION:
            warn('Incompatible Resume Manifest Version Detected.')
            inp = None
            
            while inp not in ["D", "d", "A", "a"]:
                inp = input("(D)iscard incompatible manifest or (A)bort? (D/d/A/a): ")
                
                if inp in ["D", "d"]:
                    warn("Discarding")
                    return (False, None, None)
                elif inp in ["A", "a"]:
                    warn("Aborting")
                    sys.exit()
        
        return (needresume, resumedb, resumeprops)
        
    except Exception:
        return (False, None, None)


def create_resume_properties(config, skipknown, partial, updateonly):
    """Create properties dict for resume manifest.
    
    Args:
        config: FetchConfig with download/filter settings
        skipknown: Whether skipknown mode is active
        partial: Whether partial mode is active
        updateonly: Whether updateonly mode is active
        
    Returns:
        Dict containing all parameters needed to resume the update
    """
    return {
        'resume_manifest_syntax_version': RESUME_MANIFEST_SYNTAX_VERSION,
        'os_list': config.os_list,
        'lang_list': config.lang_list,
        'installers': config.installers,
        'strict': False,  # Will be overridden by caller if needed
        'complete': False,
        'skipknown': skipknown,
        'partial': partial,
        'updateonly': updateonly,
        'strictDupe': config.strict_dupe,
        'md5xmls': config.md5xmls,
        'noChangeLogs': config.no_changelogs
    }


def process_items_with_resume(items, gamesdb, game_filter, skipknown, updateonly):
    """Process game items with periodic resume saves and strict update checking.
    
    This function handles the main update loop: processing each game item,
    applying strict update checking, and saving progress periodically for
    crash recovery.
    
    Args:
        items: List of game items to process
        gamesdb: Existing manifest database to update
        game_filter: GameFilter object with strict update flags
        skipknown: Whether in skipknown mode (affects save frequency)
        updateonly: Whether in updateonly mode (affects save frequency)
        
    Returns:
        Tuple of (updated_gamesdb, global_dupes)
        - updated_gamesdb: Updated manifest with all processed games
        - global_dupes: List of games with duplicate titles (need _id suffix)
        
    Notes:
        - Saves manifest every RESUME_SAVE_THRESHOLD games
        - Saves more frequently in skipknown/updateonly modes
        - Handles exceptions gracefully, logging and continuing
        - Updates resume manifest after each game
    """
    # Create resume tracking
    resumedb = sorted(items, key=lambda item: item.title)
    items_count = len(items)
    print_padding = len(str(items_count))
    resumedbInitLength = len(resumedb)
    i = 0
    
    # Process each item
    for item in sorted(items, key=lambda item: item.title):
        i += 1
        info("(%*d / %d) processing %s..." % (print_padding, i, items_count, item.title))
        
        try:
            # Apply strict update checking if this game already exists
            item_idx = item_checkdb(item.id, gamesdb)
            if item_idx is not None:
                handle_game_updates(gamesdb[item_idx], item, game_filter.strict, 
                                   game_filter.strict, game_filter.strict)
                gamesdb[item_idx] = item
            else:
                gamesdb.append(item)
        except Exception:
            warn("The handled exception was:")
            log_exception('error')
            warn("End exception report.")
        
        # Remove from resume list
        resumedb.remove(item)
        
        # Periodic save
        if (updateonly or skipknown or 
            (resumedbInitLength - len(resumedb)) % RESUME_SAVE_THRESHOLD == 0):
            save_manifest(gamesdb)
            save_resume_manifest(resumedb)
    
    # Handle duplicate titles (add _id suffix to folder_name)
    global_dupes = []
    sorted_gamesdb = sorted(gamesdb, key=lambda game: game.title)
    for game in sorted_gamesdb:
        if game not in global_dupes:
            index = sorted_gamesdb.index(game)
            dupes = [game]
            while (len(sorted_gamesdb) - 1 >= index + 1 and 
                   sorted_gamesdb[index + 1].title == game.title):
                dupes.append(sorted_gamesdb[index + 1])
                index = index + 1
            if len(dupes) > 1:
                global_dupes.extend(dupes)
    
    for dupe in global_dupes:
        dupe.folder_name = dupe.title + "_" + str(dupe.id)
    
    return (gamesdb, global_dupes)




def fetch_and_parse_game_details(session, game_id, game_title, config):
    """Fetch detailed game data from GOG API and parse into manifest format.
    
    Retrieves complete game information including downloads, extras, DLCs, serial
    keys, changelogs, etc. Applies content filtering based on OS/language preferences.
    
    Args:
        session: Authenticated GOG session object
        game_id: Numeric game ID from GOG
        game_title: Game title/slug for logging
        config: FetchConfig object containing download and filter settings
        
    Returns:
        AttrDict containing complete parsed game data ready for manifest, or None if fetch fails
        
    Notes:
        - Handles UTF-16 serial key decoding
        - Filters downloads by OS/language
        - Deduplicates download lists
        - Categorizes downloads into standalone/galaxy/shared lists
    """
    api_url = GOG_ACCOUNT_URL + "/gameDetails/{}.json".format(game_id)
    
    info("fetching game details for %s..." % game_title)
    
    try:
        response = request(session, api_url)
        item_json_data = response.json()
        
        # Create game item from basic product info (would be passed in from fetch_all_product_ids)
        # For now, create minimal structure - in real integration, this would be passed in
        item = AttrDict()
        item.id = game_id
        item.title = game_title
        item.long_title = item_json_data.get('title', game_title)
        
        # Parse detailed data
        item.bg_url = item_json_data['backgroundImage']
        item.bg_urls = AttrDict()
        item.serial = item_json_data['cdKey']
        
        # Handle UTF-16 encoded serial keys
        if not item.serial.isprintable():
            try:
                pserial = item.serial
                if len(pserial) % 2:  # Odd length
                    pserial = pserial + "\x00"
                pserial = bytes(pserial, "UTF-8")
                pserial = pserial.decode("UTF-16")
                if pserial.isprintable():
                    item.serial = pserial
                else:
                    warn('Game serial code is unprintable, storing raw')
            except Exception:
                warn('Game serial code is unprintable and decoding failed, storing raw')
        
        item.serials = AttrDict()
        if item.serial != '':
            item.serials[item.long_title] = item.serial
        
        item.used_titles = [item.long_title]
        item.forum_url = item_json_data['forumLink']
        
        # Handle changelogs
        if config.no_changelogs:
            item_json_data['changelog'] = ''
        item.changelog = item_json_data['changelog']
        item.changelog_end = None
        
        item.release_timestamp = item_json_data['releaseTimestamp']
        item.gog_messages = item_json_data['messages']
        
        # Initialize download lists
        item.downloads = []
        item.galaxyDownloads = []
        item.sharedDownloads = []
        item.extras = []
        
        # Store detailed GOG data
        item.detailed_gog_data = AttrDict()
        for key in item_json_data:
            if key not in ["downloads", "extras", "galaxyDownloads", "dlcs"]:
                try:
                    tmp_contents = item[key]
                    if tmp_contents != item_json_data[key]:
                        debug("Detailed GOG Data Key, %s, for item clashes" % key)
                        item.detailed_gog_data[key] = item_json_data[key]
                except Exception:
                    item[key] = item_json_data[key]
        
        # Filter downloads by OS/language
        filter_downloads(item.downloads, item_json_data['downloads'], config.lang_list, config.os_list, config.md5xmls, session)
        filter_downloads(item.galaxyDownloads, item_json_data['galaxyDownloads'], config.lang_list, config.os_list, config.md5xmls, session)
        filter_extras(item.extras, item_json_data['extras'], config.md5xmls, session)
        filter_dlcs(item, item_json_data['dlcs'], config.lang_list, config.os_list, config.md5xmls, session)
        
        # Deduplicate downloads
        item.downloads = deDuplicateList(item.downloads, {}, config.strict_dupe)
        item.galaxyDownloads = deDuplicateList(item.galaxyDownloads, {}, config.strict_dupe)
        
        # Identify shared downloads (in both standalone and galaxy)
        item.sharedDownloads = [x for x in item.downloads if x in item.galaxyDownloads]
        
        # Apply installer type filter
        if config.installers == 'galaxy':
            item.downloads = []
        else:
            item.downloads = [x for x in item.downloads if x not in item.sharedDownloads]
        
        if config.installers == 'standalone':
            item.galaxyDownloads = []
        else:
            item.galaxyDownloads = [x for x in item.galaxyDownloads if x not in item.sharedDownloads]
        
        # Final deduplication across all download types
        existing_items = {}
        item.downloads = deDuplicateList(item.downloads, existing_items, config.strict_dupe)
        item.galaxyDownloads = deDuplicateList(item.galaxyDownloads, existing_items, config.strict_dupe)
        item.sharedDownloads = deDuplicateList(item.sharedDownloads, existing_items, config.strict_dupe)
        item.extras = deDuplicateList(item.extras, existing_items, config.strict_dupe)
        
        return item
        
    except Exception as e:
        warn("Failed to fetch game details: %s" % str(e))
        return None


def update_full_library(session, config):
    """Perform a full library update - fetch and update ALL games from GOG.
    
    This is the simplest update strategy: fetch every game in the user's library
    and create/update manifest entries for all of them. Uses game_filter module
    with no active filters to ensure all games are included.
    
    Args:
        session: Authenticated GOG session object
        config: FetchConfig object containing download and filter settings
        
    Returns:
        List of game AttrDicts ready to be saved as manifest
        
    Example:
        >>> session = makeGOGSession()
        >>> config = FetchConfig(os_list=['windows'], lang_list=['en'])
        >>> manifest = update_full_library(session, config)
        >>> save_manifest(manifest)
    """
    info("Starting full library update...")
    
    # Step 1: Fetch all game IDs from product list
    products = fetch_all_product_ids(session)
    
    info('found %d games in library' % len(products))
    
    # Step 2: Create filter for full update (no game filtering)
    game_filter = create_filter_for_full_update()
    
    # Step 3: Apply filter (will return all games since no filters active)
    filtered_products = filter_game_list(products, game_filter)
    
    # Step 4: Fetch detailed data for each game
    return fetch_and_merge_manifest(session, filtered_products, config)


def update_specific_games(session, game_ids, config, skipids=None):
    """Update only specific games by ID or title.
    
    Fetches the product list, filters to only the requested games, then fetches
    detailed data for those games. This is more efficient than a full update when
    you only want to update a handful of games.
    
    Uses the game_filter module to match games by title or numeric ID.
    
    Args:
        session: Authenticated GOG session object
        game_ids: List of game titles or IDs to update (e.g., ['witcher_3', '1234567'])
        config: FetchConfig object containing download and filter settings
        skipids: Optional list of IDs to exclude (takes priority over game_ids)
        
    Returns:
        List of game AttrDicts for matching games, ready to be saved as manifest
        
    Raises:
        Warning if requested IDs are not found in library
        
    Example:
        >>> session = makeGOGSession()
        >>> config = FetchConfig(os_list=['windows'], lang_list=['en'])
        >>> manifest = update_specific_games(session, ['witcher_3', 'cyberpunk_2077'], config)
        >>> save_manifest(manifest)
    """
    info("Starting specific games update for: %s" % ', '.join(game_ids))
    
    # Step 1: Fetch all game IDs to find matches
    products = fetch_all_product_ids(session)
    
    # Step 2: Create filter for specific games
    game_filter = create_filter_for_specific_games(game_ids, skipids)
    
    # Step 3: Filter product list to only requested games
    filtered_products = filter_game_list(products, game_filter)
    
    if not filtered_products:
        warn('No games found matching requested IDs: %s' % ', '.join(game_ids))
        return []
    
    info('found %d matching game(s)' % len(filtered_products))
    
    # Report if any requested IDs were not found
    found_ids = {p.title for p in filtered_products} | {str(p.id) for p in filtered_products}
    missing_ids = [gid for gid in game_ids if gid not in found_ids]
    if missing_ids:
        warn('requested game(s) not found in library: %s' % ', '.join(missing_ids))
    
    # Step 4: Fetch detailed data for each matching game
    return fetch_and_merge_manifest(session, filtered_products, config)


def update_partial(session, known_ids, config, skipids=None, skip_hidden=False):
    """Perform a partial update - only new games OR games with updates.
    
    This is an efficient update strategy for routine maintenance. It adds games
    purchased since the last update AND updates games that GOG reports as having
    changes. Skips games already in manifest that haven't changed.
    
    Args:
        session: Authenticated GOG session object
        known_ids: List of game IDs already in manifest
        config: FetchConfig object containing download and filter settings
        skipids: Optional list of IDs to exclude
        skip_hidden: Whether to exclude hidden games
        
    Returns:
        List of game AttrDicts for new or updated games
        
    Example:
        >>> session = makeGOGSession()
        >>> old_manifest = load_manifest()
        >>> known_ids = [g.id for g in old_manifest]
        >>> config = FetchConfig(os_list=['windows'], lang_list=['en'])
        >>> updates = update_partial(session, known_ids, config)
    """
    info("Starting partial update (new games + updated games)...")
    
    # Step 1: Fetch all game IDs
    products = fetch_all_product_ids(session)
    
    # Step 2: Create filter for partial update
    game_filter = create_filter_for_partial_update(known_ids)
    if skipids:
        game_filter.skipids = skipids
    if skip_hidden:
        game_filter.skipHidden = True
    
    # Step 3: Filter to only new or updated games
    filtered_products = filter_game_list(products, game_filter)
    
    if not filtered_products:
        info('no new games or updates found')
        return []
    
    info('found %d new or updated game(s)' % len(filtered_products))
    
    # Step 4: Fetch detailed data for each filtered game
    return fetch_and_merge_manifest(session, filtered_products, config)


def update_new_games_only(session, known_ids, config, skipids=None, skip_hidden=False):
    """Update only new games not already in manifest.
    
    Adds games purchased since the last update, but ignores games already in
    the manifest even if they have updates. Useful for quickly adding new
    purchases without processing existing games.
    
    Args:
        session: Authenticated GOG session object
        known_ids: List of game IDs already in manifest
        config: FetchConfig object containing download and filter settings
        skipids: Optional list of IDs to exclude
        skip_hidden: Whether to exclude hidden games
        
    Returns:
        List of game AttrDicts for new games only
        
    Example:
        >>> session = makeGOGSession()
        >>> old_manifest = load_manifest()
        >>> known_ids = [g.id for g in old_manifest]
        >>> config = FetchConfig(os_list=['windows'])
        >>> new_games = update_new_games_only(session, known_ids, config)
    """
    info("Starting new games only update...")
    
    # Step 1: Fetch all game IDs
    products = fetch_all_product_ids(session)
    
    # Step 2: Create filter for new games only
    game_filter = create_filter_for_new_games(known_ids)
    if skipids:
        game_filter.skipids = skipids
    if skip_hidden:
        game_filter.skipHidden = True
    
    # Step 3: Filter to only new games
    filtered_products = filter_game_list(products, game_filter)
    
    if not filtered_products:
        info('no new games found')
        return []
    
    info('found %d new game(s)' % len(filtered_products))
    
    # Step 4: Fetch detailed data for each new game
    return fetch_and_merge_manifest(session, filtered_products, config)


def update_changed_games_only(session, config, skipids=None, skip_hidden=False):
    """Update only games that GOG reports as having updates.
    
    Processes only games with the has_updates flag set by GOG, ignoring both
    new games and unchanged games. Fastest way to check for updates to existing
    games in your library.
    
    Args:
        session: Authenticated GOG session object
        config: FetchConfig object containing download and filter settings
        skipids: Optional list of IDs to exclude
        skip_hidden: Whether to exclude hidden games
        
    Returns:
        List of game AttrDicts for games with updates only
        
    Example:
        >>> session = makeGOGSession()
        >>> config = FetchConfig(os_list=['windows'], lang_list=['en'])
        >>> updates = update_changed_games_only(session, config)
    """
    info("Starting updates only check...")
    
    # Step 1: Fetch all game IDs
    products = fetch_all_product_ids(session)
    
    # Step 2: Create filter for updated games only
    game_filter = create_filter_for_updated_games()
    if skipids:
        game_filter.skipids = skipids
    if skip_hidden:
        game_filter.skipHidden = True
    
    # Step 3: Filter to only games with updates
    filtered_products = filter_game_list(products, game_filter)
    
    if not filtered_products:
        info('no game updates found')
        return []
    
    info('found %d game(s) with updates' % len(filtered_products))
    
    # Step 4: Fetch detailed data for each updated game
    return fetch_and_merge_manifest(session, filtered_products, config)


def handle_single_game_rename(game, savedir, orphan_root_dir, dryrun):
    """Handle directory and file renames for a single game.
    
    When GOG changes a game's title, this function renames the local directory
    and files to match. Moves conflicting files/directories to an orphan folder
    to prevent data loss.
    
    Args:
        game: Game object with title, folder_name, old_folder_name, and downloads/extras
        savedir: Base directory where game folders are stored
        orphan_root_dir: Directory to move conflicting files/folders
        dryrun: If True, only log what would be done without making changes
    """
    # Ensure game has all required attributes (defensive programming)
    try:
        _ = game.galaxyDownloads
    except AttributeError:
        game.galaxyDownloads = []
        
    try:
        _ = game.sharedDownloads
    except AttributeError:
        game.sharedDownloads = []
        
    try:
        _ = game.old_title
    except AttributeError:
        game.old_title = None
        
    try:
        _ = game.folder_name
    except AttributeError:
        game.folder_name = game.title
        
    try:
        _ = game.old_folder_name
    except AttributeError:
        game.old_folder_name = game.old_title
    
    try:
        _ = game.downloads
    except AttributeError:
        game.downloads = []
    
    try:
        _ = game.extras
    except AttributeError:
        game.extras = []
    
    # Handle game directory rename
    if game.old_folder_name is not None:
        src_dir = os.path.join(savedir, game.old_folder_name)
        dst_dir = os.path.join(savedir, game.folder_name)
        
        if os.path.isdir(src_dir):
            try:
                if os.path.exists(dst_dir):
                    warn("orphaning destination clash '{}'".format(dst_dir))
                    if not dryrun:
                        move_with_increment_on_clash(dst_dir, os.path.join(orphan_root_dir, game.folder_name))
                
                info('  -> renaming directory "{}" -> "{}"'.format(src_dir, dst_dir))
                if not dryrun:
                    move_with_increment_on_clash(src_dir, dst_dir)
            except Exception:
                error('    -> rename failed "{}" -> "{}"'.format(game.old_folder_name, game.folder_name))
    
    # Handle file renames within the game directory
    for item in game.downloads + game.galaxyDownloads + game.sharedDownloads + game.extras:
        try:
            _ = item.old_name
        except AttributeError:
            item.old_name = None
        
        if item.old_name is not None:
            game_dir = os.path.join(savedir, game.folder_name)
            src_file = os.path.join(game_dir, item.old_name)
            dst_file = os.path.join(game_dir, item.name)
            
            if os.path.isfile(src_file):
                try:
                    if os.path.exists(dst_file):
                        warn("orphaning destination clash '{}'".format(dst_file))
                        dest_dir = os.path.join(orphan_root_dir, game.folder_name)
                        if not os.path.isdir(dest_dir):
                            if not dryrun:
                                os.makedirs(dest_dir)
                        if not dryrun:
                            move_with_increment_on_clash(dst_file, os.path.join(dest_dir, item.name))
                    
                    info('  -> renaming file "{}" -> "{}"'.format(src_file, dst_file))
                    if not dryrun:
                        move_with_increment_on_clash(src_file, dst_file)
                        item.old_name = None  # only rename once
                except Exception:
                    error('    -> rename failed "{}" -> "{}"'.format(src_file, dst_file))
                    if not dryrun:
                        item.prev_verified = False

