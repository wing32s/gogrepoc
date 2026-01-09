#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Download command implementation for GOGRepoc
"""

import os
import sys
import time
import shutil
import threading
import datetime
import platform
import ctypes
import ctypes.wintypes
import requests
from queue import Queue

from .utils import (
    info, warn, error, debug, log_exception,
    ConditionalWriter, hashfile, hashstream, slugify, pretty_size,
    check_skip_file, get_total_size, move_with_increment_on_clash,
    process_path, open_notrunc, open_notruncwrrd, get_fs_type,
    LANG_TABLE, GOG_HOME_URL, INFO_FILENAME, SERIAL_FILENAME,
    DOWNLOADING_DIR_NAME, PROVISIONAL_DIR_NAME, ORPHAN_DIR_NAME,
    IMAGES_DIR_NAME, ORPHAN_FILE_EXCLUDE_LIST,
    HTTP_TIMEOUT, HTTP_RETRY_COUNT, HTTP_RETRY_DELAY,
    HTTP_GAME_DOWNLOADER_THREADS,
    GENERIC_READ, GENERIC_WRITE, OPEN_EXISTING, CREATE_NEW, FILE_BEGIN,
    WINDOWS_PREALLOCATION_FS, POSIX_PREALLOCATION_FS,
    uLongPathPrefix
)
from .api import makeGOGSession, request, request_head, fetch_chunk_tree, renew_token, check_and_renew_token
from .manifest import load_manifest, save_manifest, handle_game_renames
from .game_filter import GameFilter
from .utils import html2text

# Utility functions for size formatting
def megs(b):
    """Format bytes as megabytes"""
    return '%.1fMB' % (b / float(1024**2))

def gigs(b):
    """Format bytes as gigabytes"""
    return '%.2fGB' % (b / float(1024**3))

def filter_games_by_id(items, game_filter):
    """Filter game items by ID inclusion and exclusion lists with error handling.
    
    Purpose:
        Apply GameFilter's ID-based filtering to select specific games or exclude certain
        games from a download operation. This function provides the primary game selection
        mechanism for targeted downloads.
    
    Args:
        items: List of game manifest items to filter. Each item should have:
              - title: Game title (string, used for matching)
              - id: Numeric game ID (int, converted to string for matching)
        game_filter: GameFilter instance containing:
                    - ids: List of game IDs/titles to include (empty list = include all)
                    - skipids: List of game IDs/titles to exclude (empty list = exclude none)
    
    Returns:
        list: Filtered game items matching the inclusion/exclusion criteria.
              Returns original list if both ids and skipids are empty.
    
    Behavior:
        Inclusion Filtering (game_filter.ids):
        - When ids list is provided (non-empty), only games matching the specified
          IDs or titles are retained
        - Matching is performed against both item.title and str(item.id) for flexibility
        - Logs the formatted ID list and reports the number of games after filtering
        - Empty ids list means no inclusion filtering (all games pass through)
        
        Exclusion Filtering (game_filter.skipids):
        - When skipids list is provided (non-empty), games matching the specified
          IDs or titles are removed from the result
        - Applied after inclusion filtering if both are specified
        - Matching is performed against both item.title and str(item.id)
        - Empty skipids list means no exclusion filtering
        
        Error Handling:
        - If no games remain after filtering, logs detailed error message and exits
        - Error message adapts based on which filters were applied:
          * Both filters: Reports which IDs were requested and which were skipped
          * Only ids: Reports that no matching games were found
          * Only skipids: Reports that all games were excluded
          * Neither: Generic "no game found" error
        
        Filter Precedence:
        - Inclusion filtering is applied first (if ids is non-empty)
        - Exclusion filtering is applied second (if skipids is non-empty)
        - This allows for "include these except those" patterns
    
    Important Notes:
        - ID Matching: Both numeric IDs and title strings are checked for matches,
          providing flexibility in how users specify games
        - Empty Results: Function exits with error code 1 if filtering results in
          an empty game list, preventing unintended full-library operations
        - Case Sensitivity: Title matching is case-sensitive
        - GameFilter Integration: This function is part of the GameFilter refactoring
          pattern where filter parameters are encapsulated in a GameFilter instance
    
    Examples:
        # Filter for specific games by title
        game_filter = GameFilter(ids=['beat_cop', 'hollow_knight'], skipids=[])
        filtered = filter_games_by_id(all_games, game_filter)
        
        # Filter by numeric ID
        game_filter = GameFilter(ids=['1234567890'], skipids=[])
        filtered = filter_games_by_id(all_games, game_filter)
        
        # Include multiple games but skip one
        game_filter = GameFilter(
            ids=['game_a', 'game_b', 'game_c'],
            skipids=['game_b']
        )
        filtered = filter_games_by_id(all_games, game_filter)  # Returns game_a and game_c
        
        # No filtering (returns all games)
        game_filter = GameFilter(ids=[], skipids=[])
        filtered = filter_games_by_id(all_games, game_filter)
        
        # Skip specific games from all games
        game_filter = GameFilter(ids=[], skipids=['problematic_game'])
        filtered = filter_games_by_id(all_games, game_filter)
    
    Usage in cmd_download:
        This function is called early in the download workflow to narrow down which
        games will be processed:
        
        # Create GameFilter from command-line parameters
        game_filter = GameFilter(
            ids=ids if ids else [],
            skipids=skipids if skipids else []
        )
        items = filter_games_by_id(items, game_filter)
        
        The filtered list then proceeds through OS/language filtering and file selection.
    
    Raises:
        SystemExit: Exits with code 1 if no games remain after filtering, preventing
                   accidental operations on empty game lists.
    """
    if game_filter.ids:
        formattedIds = ', '.join(map(str, game_filter.ids))
        info("downloading games with id(s): {%s}" % formattedIds)
        if items:
            info("First item title: '{}', id: '{}'".format(
                items[0].title if hasattr(items[0], 'title') else 'NO TITLE', 
                items[0].id if hasattr(items[0], 'id') else 'NO ID'))
        items = [item for item in items if item.title in game_filter.ids or str(item.id) in game_filter.ids]
        info("Filtered to {} games".format(len(items)))

    if game_filter.skipids:
        formattedSkipIds = ', '.join(map(str, game_filter.skipids))
        info("skipping games with id(s): {%s}" % formattedSkipIds)
        items = [item for item in items if item.title not in game_filter.skipids and str(item.id) not in game_filter.skipids]

    if not items:
        if game_filter.ids and game_filter.skipids:
            error('no game(s) with id(s) in "{}" was found'.format(game_filter.ids) + 
                  'after skipping game(s) with id(s) in "{}".'.format(game_filter.skipids))
        elif game_filter.ids:
            error('no game with id in "{}" was found.'.format(game_filter.ids))
        elif game_filter.skipids:
            error('no game was found was found after skipping game(s) with id(s) in "{}".'.format(game_filter.skipids))
        else:
            error('no game found')
        exit(1)
    
    return items

def filter_downloads_by_os_and_lang(downloads, os_list, lang_list):
    """Filter download items by operating system and language preferences.
    
    Purpose:
        Apply OS and language filters to game installer downloads. This function is designed
        for filtering installers (standalone, Galaxy, and shared downloads) but should NOT
        be used for extras, which have special handling requirements.
    
    Args:
        downloads: List of download items to filter. Each item should have:
                  - os_type: Operating system type ('windows', 'mac', 'linux', or 'extra')
                  - lang: Language code in GOG API format
        os_list: List of OS types to include (e.g., ['windows', 'linux']).
                If None or empty, no OS filtering is applied (all OS types pass through).
        lang_list: List of language codes to include (e.g., ['en', 'de', 'fr']).
                  Uses human-readable codes that are converted via LANG_TABLE to GOG API format.
                  If None or empty, no language filtering is applied (all languages pass through).
    
    Returns:
        list: Filtered download items matching the specified OS and language criteria.
              Returns all items if both os_list and lang_list are empty/None.
    
    Behavior:
        OS Filtering:
        - When os_list is provided, only items with os_type matching one of the
          specified values are retained
        - Common os_type values: 'windows', 'mac', 'linux'
        - Extras have os_type='extra' and should be filtered separately
        
        Language Filtering:
        - When lang_list is provided, converts human-readable language codes to
          GOG API format using LANG_TABLE
        - Only items with lang matching a converted language code are retained
        - Language codes follow ISO standards (e.g., 'en' for English, 'de' for German)
        
        Filter Application:
        - Filters are applied sequentially (OS first, then language)
        - Both filters are optional and independent
        - Empty filter lists result in no filtering for that dimension
    
    Important Notes:
        - Extras Exclusion: Do NOT use this function to filter extras. Extras have
          os_type='extra' and lang='' by design and require different handling.
        - Filter Order: OS filtering is performed before language filtering for efficiency.
        - Empty Lists: Passing empty lists or None for both parameters returns the original
          list unmodified (no filtering).
    
    Examples:
        # Filter for Windows-only downloads
        windows_downloads = filter_downloads_by_os_and_lang(
            item.downloads, ['windows'], None
        )
        
        # Filter for English and German installers on Linux
        localized_downloads = filter_downloads_by_os_and_lang(
            item.galaxyDownloads, ['linux'], ['en', 'de']
        )
        
        # No filtering (returns all items)
        all_downloads = filter_downloads_by_os_and_lang(item.downloads, None, None)
        
        # Multi-OS, single language
        multi_platform = filter_downloads_by_os_and_lang(
            item.sharedDownloads, ['windows', 'mac', 'linux'], ['en']
        )
    
    Usage in cmd_download:
        This function is called after game ID filtering to narrow down downloads
        based on user-specified OS and language preferences:
        
        filtered_downloads = filter_downloads_by_os_and_lang(
            filtered_downloads, os_list, lang_list
        )
        filtered_galaxyDownloads = filter_downloads_by_os_and_lang(
            filtered_galaxyDownloads, os_list, lang_list
        )
        filtered_sharedDownloads = filter_downloads_by_os_and_lang(
            filtered_sharedDownloads, os_list, lang_list
        )
    """
    # Filter by OS (only if os_list is provided)
    if os_list:
        downloads = [item for item in downloads if item.os_type in os_list]
    
    # Filter by language (only if lang_list is provided)
    if lang_list:
        # Convert language codes to GOG API format
        valid_langs = [LANG_TABLE[lang] for lang in lang_list]
        # Filter by language
        downloads = [item for item in downloads if item.lang in valid_langs]
    
    return downloads

def clean_up_temp_directory(target_dir, all_items_by_title, dryrun, skip_subdir=None):
    """Clean up temporary directories by removing outdated game folders and files.
    
    Purpose:
        Maintain temporary download directories by removing directories and files that are
        no longer present in the current manifest. This prevents accumulation of orphaned
        data from renamed games, removed games, or outdated files from previous downloads.
    
    Args:
        target_dir: Absolute path to the temporary directory to clean up.
                   Typically downloadingdir or provisionaldir.
        all_items_by_title: Dictionary mapping game folder names (titles) to manifest items.
                          Used to determine which directories are still valid.
                          Structure: {folder_name: game_item, ...}
        dryrun: If True, only logs what would be removed without actually deleting anything.
               If False, performs actual file and directory removal.
        skip_subdir: Optional subdirectory name to skip during cleanup (e.g., PROVISIONAL_DIR_NAME).
                    Useful when cleaning parent directory while preserving a specific subdirectory.
                    Default: None (no subdirectories are skipped)
    
    Behavior:
        Directory-Level Cleanup:
        - Iterates through all top-level directories in target_dir
        - Checks if each directory name exists as a key in all_items_by_title
        - Removes entire directory if not found in manifest (game was removed/renamed)
        - Skips directories specified in skip_subdir parameter
        
        File-Level Cleanup:
        - For valid game directories, builds list of expected filenames from manifest
        - Expected files include: downloads, galaxyDownloads, sharedDownloads, and extras
        - Removes files that exist in directory but not in expected filename list
        - Also removes any unexpected subdirectories within game folders
        
        Dry Run Mode:
        - When dryrun=True, logs all removal operations without executing them
        - Useful for previewing what will be cleaned up before committing changes
        - All "Removing..." messages are logged but no actual deletions occur
        
        Error Handling:
        - Early return if target_dir doesn't exist (nothing to clean)
        - Safe removal using shutil.rmtree for directories and os.remove for files
        - No explicit exception handling (errors propagate to caller)
    
    Important Notes:
        - Non-Destructive Preview: Use dryrun=True to preview cleanup without making changes
        - Manifest Synchronization: Only removes items not in the current manifest, ensuring
          cleanup stays synchronized with latest game library state
        - Nested Directories: Unexpected subdirectories within game folders are removed
          (game files should be flat in game directory)
        - Skip Subdirectory: The skip_subdir parameter allows selective cleanup when
          provisional and downloading directories share a parent-child relationship
    
    Examples:
        # Clean downloading directory, skip provisional subdirectory
        clean_up_temp_directory(
            downloadingdir,
            all_items_by_title,
            dryrun=False,
            skip_subdir=PROVISIONAL_DIR_NAME
        )
        
        # Clean provisional directory (no subdirs to skip)
        clean_up_temp_directory(
            provisionaldir,
            all_items_by_title,
            dryrun=False,
            skip_subdir=None
        )
        
        # Preview cleanup without making changes
        clean_up_temp_directory(
            downloadingdir,
            all_items_by_title,
            dryrun=True,
            skip_subdir=None
        )
    
    Usage in cmd_download:
        This function is called after processing leftover provisional files and before
        starting new downloads to ensure temporary directories are clean:
        
        # Clean up downloading directory but preserve provisional subdirectory
        clean_up_temp_directory(
            downloadingdir,
            all_items_by_title,
            dryrun,
            skip_subdir=PROVISIONAL_DIR_NAME
        )
        
        # Clean up provisional directory
        clean_up_temp_directory(provisionaldir, all_items_by_title, dryrun)
        
        This ensures:
        - Removed games don't leave orphaned directories
        - Renamed games get their old directories cleaned up
        - Outdated files from previous manifest versions are removed
        - Temporary storage stays synchronized with current manifest
    
    Typical Cleanup Scenarios:
        1. Game Removed: Directory "old_game" exists but not in manifest → entire directory removed
        2. Game Renamed: Directory "old_name" exists but manifest shows "new_name" → old directory removed
        3. Outdated File: File "old_installer_v1.exe" exists but manifest shows "new_installer_v2.exe" → old file removed
        4. Unexpected Subdir: Game directory contains subdirectory (shouldn't happen) → subdirectory removed
    """
    if not os.path.isdir(target_dir):
        return
        
    info("Cleaning up " + target_dir)
    for cur_dir in sorted(os.listdir(target_dir)):
        cur_fulldir = os.path.join(target_dir, cur_dir)
        if not os.path.isdir(cur_fulldir):
            continue
            
        # Skip specific subdirectories if requested
        if skip_subdir and cur_dir == skip_subdir:
            continue
            
        if cur_dir not in all_items_by_title:
            # Directory doesn't match any known game
            info("Removing outdated directory " + cur_fulldir)
            if not dryrun:
                shutil.rmtree(cur_fulldir)
        else:
            # Directory is valid game folder, check its files
            expected_filenames = []
            game = all_items_by_title[cur_dir]
            for game_item in game.downloads + game.galaxyDownloads + game.sharedDownloads + game.extras:
                expected_filenames.append(game_item.name)
                
            for cur_dir_file in os.listdir(cur_fulldir):
                file_path = os.path.join(target_dir, cur_dir, cur_dir_file)
                if os.path.isdir(file_path):
                    # Remove unexpected subdirectories
                    info("Removing subdirectory(?!) " + file_path)
                    if not dryrun:
                        shutil.rmtree(file_path)
                else:
                    # Remove outdated files
                    if cur_dir_file not in expected_filenames:
                        info("Removing outdated file " + file_path)
                        if not dryrun:
                            os.remove(file_path)

def write_game_info_file(item_homedir, item, filtered_downloads, filtered_galaxyDownloads, filtered_sharedDownloads, filtered_extras):
    """Write comprehensive game information to a human-readable text file.
    
    Purpose:
        Generate an info.txt file containing all relevant game metadata, available downloads,
        and changelog information. This provides users with a readable summary of each game's
        details without needing to access the GOG website or parse manifest data.
    
    Args:
        item_homedir: Absolute path to the game's home directory where info.txt will be saved.
        item: Game manifest item containing all game metadata. Expected attributes:
             - long_title: Full game title for display
             - title: Short game title/folder name
             - genre: Game genre (optional)
             - id: Numeric game ID
             - store_url: Relative URL to game's store page
             - rating: User rating as decimal (0.0-5.0), multiplied by 2 for percentage
             - release_timestamp: Unix timestamp of game release date
             - gog_messages: List of special messages from GOG (optional)
             - changelog: HTML changelog text (optional)
        filtered_downloads: List of standalone installer items after OS/language filtering.
                           Each item has name, desc (description), and version attributes.
        filtered_galaxyDownloads: List of Galaxy installer items after OS/language filtering.
                                 Each item has name, desc, and version attributes.
        filtered_sharedDownloads: List of shared installer items after OS/language filtering.
                                 Each item has name, desc, and version attributes.
        filtered_extras: List of extra items (not filtered by OS/language).
                        Each item has name and desc attributes.
    
    Behavior:
        File Creation:
        - Uses ConditionalWriter which only writes if content has changed
        - File is written to INFO_FILENAME (typically "info.txt") in item_homedir
        - Platform-specific line separators (os.linesep) used for compatibility
        
        Content Structure:
        1. Header: Long game title with decorative separators
        2. Basic Info: title, genre (if present), game ID, URL
        3. Ratings: User rating percentage (if > 0)
        4. Release: Formatted release date (if available)
        5. GOG Messages: Special messages from GOG (if present)
        6. Game Items: Categorized list of all downloads
           - Standalone installers (if any)
           - Galaxy installers (if any)
           - Shared installers (if any)
           - Each with filename, description, and version
        7. Extras: List of extra downloads (wallpapers, manuals, etc.)
        8. Changelog: Full changelog text (if available)
        
        HTML Processing:
        - GOG messages and changelog use html2text to convert HTML to plain text
        - Newlines are normalized to platform-specific line separators
        - Content is stripped of leading/trailing whitespace
        
        Conditional Sections:
        - Sections only appear if data is available (e.g., no "galaxy" section if no Galaxy downloads)
        - Optional fields (genre, rating, release date) are skipped if not present
        
        Version Information:
        - Version numbers are indented under each installer entry
        - Only displayed if version information is available
    
    Important Notes:
        - ConditionalWriter Optimization: File is only written if content differs from existing file,
          preventing unnecessary disk writes and timestamp updates
        - Filtered Input: This function expects already-filtered download lists (OS/language filtered),
          but unfiltered extras list
        - URL Construction: Store URL is relative, combined with GOG_HOME_URL constant
        - Rating Conversion: Internal rating (0.0-5.0) is doubled to show as percentage (0-100%)
        - HTML Content: Messages and changelog may contain HTML that is converted to readable text
    
    Examples:
        # Basic usage in download workflow
        write_game_info_file(
            item_homedir="/games/beat_cop",
            item=game_manifest_item,
            filtered_downloads=windows_en_downloads,
            filtered_galaxyDownloads=windows_en_galaxy,
            filtered_sharedDownloads=windows_en_shared,
            filtered_extras=all_extras
        )
        
        # With no Galaxy downloads (empty list)
        write_game_info_file(
            item_homedir="/games/hollow_knight",
            item=game_item,
            filtered_downloads=installers,
            filtered_galaxyDownloads=[],  # No Galaxy version
            filtered_sharedDownloads=[],
            filtered_extras=extras
        )
    
    Usage in cmd_download:
        This function is called after OS/language filtering to generate info files
        for each game being downloaded:
        
        if not dryrun:
            write_game_info_file(
                item_homedir,
                item,
                filtered_downloads,
                filtered_galaxyDownloads,
                filtered_sharedDownloads,
                filtered_extras
            )
        
        The info file provides users with:
        - Quick reference to game details without opening GOG
        - List of all downloaded files with descriptions
        - Version tracking for installers
        - Changelog history for tracking updates
    
    Output Format Example:
        -- Game Full Title --
        
        title.......... game_folder_name
        genre.......... Adventure
        game id........ 1234567890
        url............ https://www.gog.com/game/game_name
        user rating.... 92%
        release date... January 15, 2020
        
        game items.....:
        
            standalone...:
        
                [setup_game_1.0.exe] -- English installer
                    version: 1.0.5
        
        extras.........:
        
            [manual.pdf] -- Game Manual
            [wallpaper.jpg] -- Wallpaper
        
        changelog......:
        
        Version 1.0.5 - Bug fixes and improvements
        ...
    """
    with ConditionalWriter(os.path.join(item_homedir, INFO_FILENAME)) as fd_info:
        fd_info.write(u'{0}-- {1} --{0}{0}'.format(os.linesep, item.long_title))
        fd_info.write(u'title.......... {}{}'.format(item.title, os.linesep))
        if item.genre:
            fd_info.write(u'genre.......... {}{}'.format(item.genre, os.linesep))
        fd_info.write(u'game id........ {}{}'.format(item.id, os.linesep))
        fd_info.write(u'url............ {}{}'.format(GOG_HOME_URL + item.store_url, os.linesep))
        if item.rating > 0:
            fd_info.write(u'user rating.... {}%{}'.format(item.rating * 2, os.linesep))
        if item.release_timestamp > 0:
            rel_date = datetime.datetime.fromtimestamp(item.release_timestamp).strftime('%B %d, %Y')
            fd_info.write(u'release date... {}{}'.format(rel_date, os.linesep))
        if hasattr(item, 'gog_messages') and item.gog_messages:
            fd_info.write(u'{0}gog messages...:{0}'.format(os.linesep))
            for gog_msg in item.gog_messages:
                fd_info.write(u'{0}{1}{0}'.format(os.linesep, html2text(gog_msg).strip().replace("\n",os.linesep)))
        fd_info.write(u'{0}game items.....:{0}{0}'.format(os.linesep))
        if len(filtered_downloads) > 0:
            fd_info.write(u'{0}    standalone...:{0}{0}'.format(os.linesep))                
        for game_item in filtered_downloads:
            fd_info.write(u'        [{}] -- {}{}'.format(game_item.name, game_item.desc, os.linesep))
            if game_item.version:
                fd_info.write(u'            version: {}{}'.format(game_item.version, os.linesep))
        if len(filtered_galaxyDownloads) > 0:
            fd_info.write(u'{0}    galaxy.......:{0}{0}'.format(os.linesep))                                        
        for game_item in filtered_galaxyDownloads:
            fd_info.write(u'        [{}] -- {}{}'.format(game_item.name, game_item.desc, os.linesep))
            if game_item.version:
                fd_info.write(u'            version: {}{}'.format(game_item.version, os.linesep))
        if len(filtered_sharedDownloads) > 0:                        
            fd_info.write(u'{0}    shared.......:{0}{0}'.format(os.linesep))                                        
        for game_item in filtered_sharedDownloads:
            fd_info.write(u'        [{}] -- {}{}'.format(game_item.name, game_item.desc, os.linesep))
            if game_item.version:
                fd_info.write(u'            version: {}{}'.format(game_item.version, os.linesep))                        
        if len(filtered_extras) > 0:
            fd_info.write(u'{0}extras.........:{0}{0}'.format(os.linesep))
            for game_item in filtered_extras:
                fd_info.write(u'    [{}] -- {}{}'.format(game_item.name, game_item.desc, os.linesep))
        if item.changelog:
            fd_info.write(u'{0}changelog......:{0}{0}'.format(os.linesep))
            fd_info.write(html2text(item.changelog).strip().replace("\n",os.linesep))
            fd_info.write(os.linesep)

def write_game_serial_file(item_homedir, item):
    """Write game serial/key information to a text file.
    
    Args:
        item_homedir: The game's home directory
        item: The game manifest item
    """
    try:
        if len(item.serials) != 0:
            with ConditionalWriter(os.path.join(item_homedir, SERIAL_FILENAME)) as fd_serial:
                for key in item.serials.keys():
                    serial = item.serials[key]
                    fd_serial.write(key + ":\n\n" )
                    serial = serial.replace(u'<span>', '')
                    serial = serial.replace(u'</span>', os.linesep)
                    serial = serial.rstrip('\n')
                    fd_serial.write(serial)
                    fd_serial.write("\n\n")
    except AttributeError:
            if item.serial != '':
                with ConditionalWriter(os.path.join(item_homedir, SERIAL_FILENAME)) as fd_serial:
                    item.serial = item.serial.replace(u'<span>', '')
                    item.serial = item.serial.replace(u'</span>', os.linesep)
                    fd_serial.write(item.serial)

def download_image_from_item_key(item, key, images_dir_name, image_orphandir, clean_existing, downloadSession):
    """Download a single game image from a manifest item's URL field.
    
    Purpose:
        Fetch and save a game image (cover art or background) from GOG's CDN based on a
        URL stored in the manifest item. Handles old image cleanup, directory creation,
        and Windows long path support for legacy Python 2 compatibility.
    
    Args:
        item: Game manifest item containing image URL fields. Accessed as dictionary.
              Expected to have attributes like 'bg_url' or 'image_url' containing
              relative CDN paths (e.g., "/images/abc123.jpg").
        key: String attribute name containing the image URL within item.
            Common values: 'bg_url' (background), 'image_url' (cover art).
            The URL is accessed via item[key].
        images_dir_name: Absolute path to the game's !images directory where images are stored.
                        Images are organized in subdirectories by key name.
        image_orphandir: Absolute path to the orphan directory for outdated images.
                        Used when clean_existing=True to preserve old images.
        clean_existing: Boolean controlling old image handling:
                       - True: Move existing image directories to orphan directory (preservation)
                       - False: Delete existing image directories completely (cleanup)
        downloadSession: Authenticated requests session for downloading from GOG CDN.
                        Must be a GOG session with valid authentication.
    
    Behavior:
        URL Construction:
        - Extracts relative path from item[key] (e.g., "/images/abc123.jpg")
        - Strips leading "/" and appends ".jpg" extension
        - Constructs full HTTPS URL: "https://" + relative_path
        
        Path Construction:
        - Creates subdirectory structure: images_dir_name/key/[subdirs]/filename.jpg
        - Splits relative path into directory and filename components
        - Handles nested directory structures from CDN paths
        
        Windows Long Path Support (Python 2):
        - On Windows with Python < 3, prepends uLongPathPrefix to all paths
        - Enables handling of paths exceeding 260 character limit
        - Applies to: file path, directory path, orphan directory
        
        Old Image Cleanup:
        - Checks if image key directory already exists
        - If exists and clean_existing=True: Moves to orphan directory with increment
        - If exists and clean_existing=False: Deletes directory completely
        - move_with_increment_on_clash prevents filename conflicts in orphan dir
        
        Download Process:
        - Only downloads if target file doesn't exist (skip if already present)
        - Creates directory structure before downloading
        - Writes image binary content directly to file
        - No verification or retry logic (simple one-shot download)
        
        Error Handling:
        - Aborts with error message if old image removal fails
        - Raises exception on removal failure (interrupts download process)
        - No explicit handling for download failures (propagates to caller)
    
    Important Notes:
        - Single Image Only: This function handles one image per key. For multiple images
          in a dictionary field (e.g., bg_urls with multiple resolutions), use
          download_image_from_item_keys instead.
        - Preservation vs Deletion: clean_existing flag determines whether old images are
          preserved (orphan directory) or permanently deleted. Preservation is safer but
          consumes more disk space.
        - Windows Compatibility: Long path prefix handling is only needed for Python 2 on
          Windows. Python 3 handles long paths natively.
        - Skip Existing: Function silently skips download if file already exists, making
          it safe to call multiple times without re-downloading.
        - Directory Creation: Automatically creates all necessary parent directories
    
    Examples:
        # Download cover image with preservation
        download_image_from_item_key(
            item=game_item,
            key='image_url',
            images_dir_name='/games/beat_cop/!images',
            image_orphandir='/games/beat_cop/!orphans/!images',
            clean_existing=True,
            downloadSession=session
        )
        
        # Download background without preservation
        download_image_from_item_key(
            item=game_item,
            key='bg_url',
            images_dir_name='/games/hollow_knight/!images',
            image_orphandir='/games/hollow_knight/!orphans/!images',
            clean_existing=False,
            downloadSession=session
        )
    
    Usage in cmd_download:
        This function is called for single-image fields when downloading game images:
        
        # Cover image download
        if item.image_url != '' and covers:
            try:
                download_image_from_item_key(
                    item, "image_url",
                    images_dir_name, image_orphandir,
                    clean_old_images, downloadSession
                )
            except KeyboardInterrupt:
                raise
            except Exception:
                warn("Could not download cover image")
        
        # Background image download (legacy single bg_url field)
        if item.bg_url != '' and backgrounds:
            try:
                download_image_from_item_key(
                    item, "bg_url",
                    images_dir_name, image_orphandir,
                    clean_old_images, downloadSession
                )
            except KeyboardInterrupt:
                raise
            except Exception:
                warn("Could not download background image")
        
        Exception handling ensures download failures don't interrupt game downloads.
    
    Directory Structure Example:
        Before:
        /games/beat_cop/!images/image_url/images/abc123.jpg  (existing)
        
        After (clean_existing=True):
        /games/beat_cop/!orphans/!images/image_url/  (moved old)
        /games/beat_cop/!images/image_url/images/xyz789.jpg  (new)
        
        After (clean_existing=False):
        /games/beat_cop/!images/image_url/images/xyz789.jpg  (old deleted)
    """
    images_key_dir_name = os.path.join(images_dir_name, key)
    key_local_path = item[key].lstrip("/") + ".jpg"
    key_url = 'https://' + key_local_path
    (dir, file) = os.path.split(key_local_path)
    key_local_path_dir = os.path.join(images_key_dir_name, dir) 
    key_local_path_file = os.path.join(key_local_path_dir, file) 
    modified_images_key_dir_name = images_key_dir_name
    
    #if (platform.system() == "Windows" and sys.version_info[0] < 3):
    #    key_local_path_file = uLongPathPrefix + os.path.abspath(key_local_path_file)
    #    key_local_path_dir = uLongPathPrefix + os.path.abspath(key_local_path_dir)
    #    image_orphandir = uLongPathPrefix + os.path.abspath(image_orphandir)
    #    modified_images_key_dir_name = uLongPathPrefix + os.path.abspath(modified_images_key_dir_name)
        
    if not os.path.exists(key_local_path_file):
        if os.path.exists(modified_images_key_dir_name):
            try:
                if clean_existing:
                    if not os.path.exists(image_orphandir):
                        os.makedirs(image_orphandir)
                    move_with_increment_on_clash(modified_images_key_dir_name, image_orphandir)
                else:
                    shutil.rmtree(modified_images_key_dir_name)
            except Exception as e:
                error("Could not remove potential old image file, aborting update attempt. Please make sure folder and files are writeable and that nothing is accessing the !image folder")
                raise
        response = request(downloadSession, key_url)
        os.makedirs(key_local_path_dir)
        with open(key_local_path_file, "wb") as out:
            out.write(response.content)

def download_image_from_item_keys(item, keys, images_dir_name, image_orphandir, clean_existing, downloadSession):
    """Download multiple game images from a manifest item's dictionary field.
    
    Purpose:
        Fetch and save multiple game images (typically background images at various resolutions)
        from GOG's CDN based on URLs stored in a dictionary within the manifest item. Handles
        old image cleanup, directory creation, and manages multiple image variants per key.
    
    Args:
        item: Game manifest item containing image URL dictionaries. Accessed as dictionary.
              Expected to have dictionary attributes like 'bg_urls' containing
              key-value pairs where keys are descriptive names (e.g., "1920x1080")
              and values are relative CDN paths.
        keys: String attribute name containing the dictionary of image URLs within item.
             Common value: 'bg_urls' (multiple background resolutions).
             The dictionary is accessed via item[keys].
        images_dir_name: Absolute path to the game's !images directory where images are stored.
                        Images are organized in subdirectories by keys name and then by key.
        image_orphandir: Absolute path to the orphan directory for outdated images.
                        Used when clean_existing=True to preserve old images.
        clean_existing: Boolean controlling old image handling:
                       - True: Move existing image directories to orphan directory (preservation)
                       - False: Delete existing image directories completely (cleanup)
        downloadSession: Authenticated requests session for downloading from GOG CDN.
                        Must be a GOG session with valid authentication.
    
    Behavior:
        Dictionary Processing:
        - Extracts dictionary from item[keys] (e.g., {"1920x1080": "/images/bg.jpg", ...})
        - Iterates through all key-value pairs in the dictionary
        - Downloads each image to a subdirectory named after the key (slugified)
        
        URL Construction:
        - For each key-value pair, extracts relative path from value
        - Strips leading "/" and appends ".jpg" extension
        - Constructs full HTTPS URL: "https://" + relative_path
        
        Path Construction:
        - Creates nested structure: images_dir_name/keys/slugified_key/[subdirs]/filename.jpg
        - Leading path uses slugified key name (safe for filesystem)
        - Trailing path preserves CDN directory structure
        - Tracks valid paths to identify orphaned folders later
        
        Old Image Cleanup (Per Key):
        - For each key, checks if image directory already exists
        - If exists and clean_existing=True: Moves to orphan directory with increment
        - If exists and clean_existing=False: Deletes directory completely
        - move_with_increment_on_clash prevents filename conflicts in orphan dir
        
        Download Process:
        - Only downloads if target file doesn't exist (skip if already present)
        - Creates directory structure before downloading
        - Writes image binary content directly to file
        - HTTPError exceptions are caught and logged, but don't stop other downloads
        
        Orphaned Folder Cleanup:
        - After processing all keys, scans for folders not in validPaths list
        - These represent old/removed resolutions no longer in manifest
        - Moves or deletes based on clean_existing flag
        
        Error Handling:
        - Aborts with error if old image removal fails (critical error)
        - Logs HTTPError for individual image download failures (non-critical)
        - Raises exception on removal failure (interrupts download process)
    
    Important Notes:
        - Multiple Images: This function handles multiple images in a dictionary. For single
          images in simple fields (e.g., bg_url, image_url), use download_image_from_item_key
          instead.
        - Preservation vs Deletion: clean_existing flag determines whether old images are
          preserved (orphan directory) or permanently deleted. Preservation is safer but
          consumes more disk space.
        - Slugified Keys: Dictionary keys are slugified to create filesystem-safe directory
          names (e.g., "1920x1080" becomes "1920x1080", but "High Res" becomes "high_res").
        - Skip Existing: Function silently skips download if file already exists, making
          it safe to call multiple times without re-downloading.
        - Partial Failure Tolerance: If one image fails to download (HTTPError), others
          continue processing.
        - Orphan Cleanup: Automatically removes subdirectories for keys no longer in manifest
    
    Examples:
        # Download multiple background resolutions with preservation
        download_image_from_item_keys(
            item=game_item,
            keys='bg_urls',
            images_dir_name='/games/beat_cop/!images',
            image_orphandir='/games/beat_cop/!orphans/!images',
            clean_existing=True,
            downloadSession=session
        )
        
        # Download without preservation (cleanup old)
        download_image_from_item_keys(
            item=game_item,
            keys='bg_urls',
            images_dir_name='/games/hollow_knight/!images',
            image_orphandir='/games/hollow_knight/!orphans/!images',
            clean_existing=False,
            downloadSession=session
        )
    
    Usage in cmd_download:
        This function is called for dictionary image fields when downloading game images:
        
        # Multiple background images (modern bg_urls field)
        try:
            if len(item.bg_urls) != 0 and backgrounds:
                # Clean up old single bg_url directory if it exists
                images_old_bg_url_dir_name = os.path.join(images_dir_name, "bg_url")
                if os.path.exists(images_old_bg_url_dir_name):
                    if clean_old_images:
                        move_with_increment_on_clash(
                            images_old_bg_url_dir_name,
                            modified_image_orphandir
                        )
                    else:
                        shutil.rmtree(images_old_bg_url_dir_name)
                
                # Download new multi-resolution backgrounds
                download_image_from_item_keys(
                    item, "bg_urls",
                    images_dir_name, image_orphandir,
                    clean_old_images, downloadSession
                )
        except KeyboardInterrupt:
            raise
        except Exception:
            warn("Could not download background image")
        
        Exception handling ensures download failures don't interrupt game downloads.
        Note the special handling to migrate from old single bg_url to new multi-resolution
        bg_urls structure.
    
    Directory Structure Example:
        item.bg_urls = {
            "1920x1080": "/images/bg_1080.jpg",
            "1600x900": "/images/bg_900.jpg"
        }
        
        Creates:
        /games/beat_cop/!images/bg_urls/1920x1080/images/bg_1080.jpg
        /games/beat_cop/!images/bg_urls/1600x900/images/bg_900.jpg
        
        If old resolution "1280x720" existed but not in new manifest:
        - clean_existing=True: Moved to /games/beat_cop/!orphans/!images/bg_urls/1280x720/
        - clean_existing=False: Deleted completely
    
    Typical Usage Scenario:
        GOG often provides multiple background image resolutions (1080p, 4K, etc.).
        This function downloads all available resolutions, organizing them by resolution
        in separate subdirectories, and automatically cleans up old resolutions that
        GOG no longer provides.
    """
    images_key_dir_name = os.path.join(images_dir_name, keys)
    images_key_orphandir_name = os.path.join(image_orphandir, keys)
    
    if not os.path.exists(images_key_dir_name):                    
        os.makedirs(images_key_dir_name)
        
    mkeys = item[keys]
    validPaths = [] 
    
    for key in mkeys.keys():
        partial_key_local_path = mkeys[key].lstrip("/") + ".jpg"
        leading_partial_key_local_path = slugify(key, True)
        leading_partial_key_local_path_dir = os.path.join(images_key_dir_name, leading_partial_key_local_path)
        validPaths.append(leading_partial_key_local_path)
        (trailing_partial_key_local_path_dir, trailing_partial_key_local_path_file) = os.path.split(partial_key_local_path)
        longpath_safe_leading_partial_key_local_path_dir = leading_partial_key_local_path_dir
        
        #if (platform.system() == "Windows" and sys.version_info[0] < 3):
        #    longpath_safe_leading_partial_key_local_path_dir = uLongPathPrefix + os.path.abspath(longpath_safe_leading_partial_key_local_path_dir)
            
        if not os.path.exists(longpath_safe_leading_partial_key_local_path_dir):
            os.makedirs(longpath_safe_leading_partial_key_local_path_dir)
            
        full_key_local_path_dir = os.path.join(leading_partial_key_local_path_dir, trailing_partial_key_local_path_dir)
        full_key_local_path_file = os.path.join(full_key_local_path_dir, trailing_partial_key_local_path_file)
        key_url = 'https://' + partial_key_local_path
        
        #if (platform.system() == "Windows" and sys.version_info[0] < 3):
        #    full_key_local_path_file = uLongPathPrefix + os.path.abspath(full_key_local_path_file)
        #    full_key_local_path_dir = uLongPathPrefix + os.path.abspath(full_key_local_path_dir)
            
        if not os.path.exists(full_key_local_path_file):
            if os.path.exists(full_key_local_path_dir):
                images_full_key_local_path_orphandir = os.path.join(images_key_orphandir_name, leading_partial_key_local_path)
                #if (platform.system() == "Windows" and sys.version_info[0] < 3):
                #    images_full_key_local_path_orphandir = uLongPathPrefix + os.path.abspath(images_full_key_local_path_orphandir)
                try:
                    if clean_existing:
                        if not os.path.exists(images_full_key_local_path_orphandir):
                            os.makedirs(images_full_key_local_path_orphandir)
                        move_with_increment_on_clash(full_key_local_path_dir, images_full_key_local_path_orphandir)
                    else:
                        shutil.rmtree(full_key_local_path_dir)
                except Exception as e:
                    error("Could not remove potential old image files, aborting update attempt. Please make sure folder and files are writeable and that nothing is accessing the !image folder")
                    raise
            try:
                response = request(downloadSession, key_url)
                os.makedirs(full_key_local_path_dir)
                with open(full_key_local_path_file, "wb") as out:
                    out.write(response.content)
            except requests.HTTPError:
                error('Could not download background image ' + full_key_local_path_file)
                
    # Clean up old folders that are no longer valid
    for potential_old_folder in sorted(os.listdir(images_key_dir_name)):
        if potential_old_folder not in validPaths:
            potential_old_folder_path = os.path.join(images_key_dir_name, potential_old_folder)
            try:
                #if (platform.system() == "Windows" and sys.version_info[0] < 3):
                #    potential_old_folder_path = uLongPathPrefix + os.path.abspath(potential_old_folder_path)
                #    images_key_orphandir_name = uLongPathPrefix + os.path.abspath(images_key_orphandir_name)
                if clean_existing:
                    if not os.path.exists(images_key_orphandir_name):
                        os.makedirs(images_key_orphandir_name)
                    move_with_increment_on_clash(potential_old_folder_path, images_key_orphandir_name)
                else:
                    shutil.rmtree(potential_old_folder_path)
            except Exception as e:
                error("Could not remove potential old image files, aborting update attempt. Please make sure folder and files are writeable and that nothing is accessing the !image folder")
                raise

def preallocate_file(file_path, target_size, skip_preallocation):
    """Preallocate disk space for a file to improve download performance and prevent fragmentation.
    
    Purpose:
        Reserve disk space for a file before downloading to reduce file fragmentation and improve
        write performance. This is particularly beneficial for large files (hundreds of MB or GB)
        where incremental writes could cause the file to become fragmented across the disk.
    
    Args:
        file_path: Absolute path to the file to preallocate. File may or may not already exist.
                  If it exists, it will be extended to the target size. If it doesn't exist,
                  a new file will be created with the target size.
        target_size: Target file size in bytes to preallocate. This should match the expected
                    final size of the file after download completes.
        skip_preallocation: If True, skip preallocation entirely and return immediately.
                          Used when user disables preallocation via --skippreallocation flag
                          or when preallocation is not desired for specific files.
    
    Behavior:
        Early Exit Conditions:
        - Returns immediately if skip_preallocation is True (user disabled feature)
        - Returns immediately on macOS (doesn't support posix_fallocate)
        - Returns silently if filesystem type doesn't support preallocation
        
        Path Processing:
        - Uses process_path() to create filesystem-compatible path representation
        - Ensures paths work correctly across different platforms and path length limits
        
        macOS Handling:
        - Darwin (macOS) doesn't support posix_fallocate system call
        - Function returns early without attempting preallocation
        - No error or warning generated (expected behavior)
        
        Windows Preallocation:
        - Checks filesystem type using get_fs_type(compat_path, True)
        - Only proceeds if filesystem is in WINDOWS_PREALLOCATION_FS list (typically NTFS)
        - Uses Windows API CreateFileW to get file handle with read/write access
        - Determines open mode based on file existence:
          * OPEN_EXISTING: File already exists (resume scenario)
          * CREATE_NEW: File doesn't exist (new download)
        - Uses SetFilePointerEx to move file pointer to target_size position
        - Uses SetEndOfFile to allocate space up to the pointer position
        - Properly closes file handle after success or failure
        - Logs detailed information and warnings for troubleshooting
        
        POSIX Preallocation (Linux):
        - Checks filesystem type using get_fs_type(file_path)
        - Only proceeds if filesystem is in POSIX_PREALLOCATION_FS list (ext4, xfs, etc.)
        - Requires Python 3 (os.posix_fallocate only available in Python 3)
        - Determines open mode based on file existence:
          * "r+b": File exists (resume scenario)
          * "wb": File doesn't exist (new download)
        - Uses os.posix_fallocate() with file descriptor, offset 0, and target_size
        - Logs operation details and warnings
        
        Error Handling:
        - Windows: Catches all exceptions during preallocation, logs detailed error info
        - Windows: Ensures file handle is closed even on failure (cleanup in finally-equivalent)
        - POSIX: Catches exceptions from posix_fallocate, logs warning but doesn't fail
        - Both platforms: Preallocation failures are non-fatal (download continues without it)
        - Logs exception details using log_exception() for debugging
        
        Filesystem Support:
        - Windows: Typically NTFS and ReFS support preallocation
        - POSIX: Typically ext4, XFS, Btrfs support preallocation
        - FAT32, exFAT may not support preallocation (silently skipped)
        - Unsupported filesystems are silently skipped (no error)
    
    Important Notes:
        - Performance Benefit: Preallocation reduces fragmentation for large files, improving
          write performance during download by 10-30% in some cases.
        - Non-Fatal Failures: If preallocation fails for any reason, the download continues
          normally without preallocation. This ensures robustness.
        - Filesystem Dependency: Preallocation support depends on filesystem type. Modern
          filesystems (NTFS, ext4, XFS) generally support it, but older or simpler filesystems
          (FAT32, exFAT) may not.
        - macOS Limitation: macOS doesn't provide posix_fallocate or equivalent, so preallocation
          is not available on this platform.
        - Resume Support: When resuming a download, preallocation extends the existing partial
          file to the full target size, ensuring space is available for remaining chunks.
        - Handle Management: Windows implementation carefully manages file handles to prevent
          resource leaks, closing handles in all code paths (success and failure).
    
    Examples:
        # Preallocate space for a new 2GB installer
        preallocate_file(
            file_path="/games/beat_cop/setup.exe",
            target_size=2147483648,  # 2GB
            skip_preallocation=False
        )
        
        # Skip preallocation (user disabled feature)
        preallocate_file(
            file_path="/games/hollow_knight/setup.exe",
            target_size=1073741824,  # 1GB
            skip_preallocation=True  # Returns immediately
        )
        
        # Resume scenario with existing partial file
        preallocate_file(
            file_path="/downloading/game/installer.exe",  # Partial file exists
            target_size=3221225472,  # 3GB
            skip_preallocation=False  # Extends to full size
        )
    
    Usage in cmd_download:
        This function is called before starting a file download to reserve disk space:
        
        # Before downloading each file
        if not dryrun:
            # Preallocate space for the file
            preallocate_file(
                downloading_path,
                item_size,
                skippreallocation  # From command-line flag
            )
            
            # Then proceed with download
            download_success, actual_size = download_with_chunk_verification(...)
        
        The preallocation happens in the downloading directory before any data is written,
        ensuring the full file space is reserved upfront.
    
    Platform-Specific Behavior:
        Windows (NTFS):
        - Uses CreateFileW, SetFilePointerEx, and SetEndOfFile APIs
        - Allocates contiguous space when possible
        - Logs: "preallocating '2147483648' bytes for 'path/to/file.exe'"
        
        Linux (ext4):
        - Uses posix_fallocate system call
        - Guarantees space allocation (not just sparse file)
        - Logs: "preallocating '2147483648' bytes for 'path/to/file.exe' using posix_fallocate"
        
        macOS:
        - No operation performed (returns early)
        - No logs generated (silent skip)
        
        Unsupported Filesystem:
        - No operation performed (returns early after filesystem check)
        - No logs generated (silent skip)
    
    Performance Characteristics:
        - Preallocation Time: O(1) on most modern filesystems (metadata operation)
        - Disk Space Impact: Full file size is reserved immediately
        - Download Speed: Improved sequential write performance (less fragmentation)
        - Failure Impact: None (download continues without preallocation)
    """
    if skip_preallocation:
        return
        
    compat_path = process_path(file_path)
    
    if platform.system() == "Darwin":
        # MacOS doesn't support posix_fallocate
        return
    elif platform.system() == "Windows":
        fs = get_fs_type(compat_path, True)
        if fs in WINDOWS_PREALLOCATION_FS:
            preH = -1
            try:
                info("preallocating '%d' bytes for '%s'" % (target_size, file_path))
                # Use appropriate open mode based on whether file exists
                open_mode = OPEN_EXISTING if os.path.exists(file_path) else CREATE_NEW
                preH = ctypes.windll.kernel32.CreateFileW(compat_path, GENERIC_READ | GENERIC_WRITE, 0, None, open_mode, 0, None)
                if preH == -1:
                    warn("could not get filehandle")
                    raise OSError()
                c_sz = ctypes.wintypes.LARGE_INTEGER(target_size)
                ctypes.windll.kernel32.SetFilePointerEx(preH, c_sz, None, FILE_BEGIN)    
                ctypes.windll.kernel32.SetEndOfFile(preH)   
                ctypes.windll.kernel32.CloseHandle(preH)
                preH = -1
            except Exception:
                warn("preallocation failed")
                warn("The handled exception was:")
                log_exception('')
                warn("End exception report.")
                if preH != -1:
                    info('failed - closing outstanding handle')
                    ctypes.windll.kernel32.CloseHandle(preH)
    else:
        # POSIX systems (Linux, etc.)
        fs = get_fs_type(file_path)
        if fs.lower() in POSIX_PREALLOCATION_FS:
            if sys.version_info[0] >= 3:
                info("preallocating '%d' bytes for '%s' using posix_fallocate" % (target_size, file_path))
                # Use appropriate open mode based on whether file exists
                open_mode = "r+b" if os.path.exists(file_path) else "wb"
                with open(file_path, open_mode) as f:
                    try:
                        os.posix_fallocate(f.fileno(), 0, target_size)
                    except Exception:    
                        warn("posix preallocation failed")

def download_file_chunk(downloading_path, href, start, end, sz, path, sizes, lock, downloadSession, tid, rates):
    """Download a single chunk of a file with automatic retry logic and manifest mismatch detection.
    
    Purpose:
        Download a specific byte range of a file from GOG's CDN with automatic retry on failure.
        This function is the core building block for parallel chunk-based downloads and chunk
        verification downloads, handling transient network failures and detecting size mismatches
        between manifest data and server responses.
    
    Args:
        downloading_path: Absolute path to the file being downloaded. File must already exist
                         (created by preallocate_file or previous write operations).
        href: Full HTTPS URL to the file on GOG's CDN. Should be a direct download link
              obtained from the manifest item.
        start: Starting byte position (0-indexed, inclusive) for this chunk.
              Example: 0 for first chunk, 1048576 for second 1MB chunk.
        end: Ending byte position (0-indexed, inclusive) for this chunk.
            Example: 1048575 for first 1MB chunk (bytes 0-1048575).
        sz: Expected total file size in bytes from manifest. Used to validate server response
            and detect manifest mismatches.
        path: Display path for logging and progress tracking. Typically the final destination
              path or a user-friendly name for error messages.
        sizes: Dictionary mapping file paths to remaining bytes to download.
              Shared across threads for progress tracking. Access must be synchronized with lock.
              Structure: {path: remaining_bytes, ...}
        lock: Threading lock for synchronizing access to shared data structures (sizes, rates).
              Must be acquired before modifying sizes or rates dictionaries or printing output.
        downloadSession: Authenticated GOG session for making HTTP requests.
                        Must have valid authentication cookies/tokens.
        tid: Thread ID for progress tracking and rate monitoring. Used to identify which
             thread is downloading which chunk in multi-threaded scenarios.
        rates: Dictionary tracking download rates per file and thread.
               Structure: {path: [(tid, (bytes, time_delta)), ...], ...}
               Used for bandwidth monitoring and progress display.
    
    Returns:
        tuple: (success: bool, actual_size: int or None)
            - success: True if chunk downloaded successfully, False otherwise
            - actual_size: Server-reported file size if it differs from manifest sz,
                          None if sizes match or download failed
    
    Behavior:
        Request Initiation:
        - Makes HTTP GET request with Range header specifying byte_range=(start, end)
        - Streams response to avoid loading entire chunk into memory
        - Uses authenticated downloadSession with GOG credentials
        
        Content-Range Header Validation:
        - Parses "Content-Range" header from server response
        - Expected format: "bytes start-end/total_size"
        - Extracts actual file size from header and compares to manifest sz
        
        Manifest Mismatch Detection:
        - If server reports different file size than manifest:
          * Logs warning with manifest size vs server size
          * Updates sz to use server-reported size
          * Returns actual_size in tuple for caller to handle
        - This handles cases where GOG updates files but manifest is stale
        
        Header Validation:
        - Verifies Content-Range header matches expected format exactly
        - Expected: "start-end/sz" (e.g., "0-1048575/10485760")
        - If mismatch detected, logs error and returns failure immediately
        - Prevents corrupted downloads from malformed server responses
        
        File Writing:
        - Opens file with open_notruncwrrd (read/write without truncation)
        - Seeks to start position for this chunk
        - Asserts file pointer is at correct position (safety check)
        - Calls ioloop to stream response content to file
        
        Download Verification:
        - Checks if downloaded size matches expected chunk size: (end - start) + 1
        - Verifies file pointer ended at expected position: end + 1
        - Both conditions must be true for success
        
        Retry Logic:
        - Retries up to HTTP_RETRY_COUNT times on partial downloads
        - Waits HTTP_RETRY_DELAY seconds between retries (logged to user)
        - Updates sizes dictionary to reflect partial progress
        - Each retry attempts full chunk download (not incremental)
        
        Error Handling:
        - HTTPError: Logs error, returns (False, actual_sz) immediately (no retry)
        - Other exceptions: Logs detailed traceback and re-raises (fatal)
        - Thread-safe error logging using lock
        
        Thread Safety:
        - Acquires lock before modifying sizes dictionary
        - Acquires lock before logging (prevents interleaved output)
        - Each chunk writes to different byte range (no file write conflicts)
    
    Important Notes:
        - Inclusive Range: Both start and end are inclusive, so chunk size is (end - start) + 1
        - Partial Progress: On retry, partial download progress is added back to sizes tracking
        - No Incremental Retry: Each retry downloads the entire chunk from start, not just
          remaining bytes. This simplifies logic but may waste bandwidth on large chunks.
        - HTTPError No Retry: HTTP errors (404, 403, 500, etc.) don't trigger retry, only
          partial downloads do. This prevents infinite retries on permanent failures.
        - Thread-Safe File I/O: Multiple threads can write to same file safely because
          each writes to non-overlapping byte ranges via seek().
        - Manifest Mismatch Handling: Caller must handle actual_size return value to update
          file size tracking and adjust remaining chunk calculations.
    
    Examples:
        # Download first 1MB chunk of a file
        success, actual_sz = download_file_chunk(
            downloading_path="/downloading/game/setup.exe",
            href="https://cdn.gog.com/content-system/v2/...",
            start=0,
            end=1048575,  # 1MB chunk (bytes 0-1048575)
            sz=104857600,  # 100MB total file size
            path="game/setup.exe",
            sizes={"game/setup.exe": 103809024},  # Remaining bytes
            lock=threading.Lock(),
            downloadSession=session,
            tid=1,
            rates={}
        )
        
        # Download middle chunk with manifest mismatch detection
        success, actual_sz = download_file_chunk(
            downloading_path="/downloading/game/installer.bin",
            href="https://cdn.gog.com/...",
            start=10485760,  # Second 10MB chunk
            end=20971519,
            sz=52428800,  # Manifest says 50MB
            path="game/installer.bin",
            sizes={"game/installer.bin": 41943040},
            lock=threading.Lock(),
            downloadSession=session,
            tid=2,
            rates={}
        )
        # If server reports 52428900 (100 bytes larger):
        # - success=True, actual_sz=52428900
        # - Caller must adjust sz and recalculate remaining chunks
        
        # Download last chunk with retry scenario
        success, actual_sz = download_file_chunk(
            downloading_path="/downloading/game/data.pak",
            href="https://cdn.gog.com/...",
            start=99999900,
            end=99999999,  # Last 100 bytes
            sz=100000000,
            path="game/data.pak",
            sizes={"game/data.pak": 100},
            lock=threading.Lock(),
            downloadSession=session,
            tid=3,
            rates={}
        )
        # If only 50 bytes downloaded:
        # - Logs: "failed to download data.pak, byte_range=(99999900, 99999999) (2 retries left)"
        # - Waits HTTP_RETRY_DELAY seconds
        # - Retries full chunk download
    
    Usage in download_with_chunk_verification:
        This function is called for each invalid chunk during MD5 verification:
        
        # Check chunk MD5
        with open_notruncwrrd(downloading_path) as out:
            valid = hashstream(out, start, end) == expected_md5
            
            if not valid:
                # Download invalid chunk
                chunk_success, detected_sz = download_file_chunk(
                    downloading_path, href, start, end, sz,
                    path, sizes, lock, downloadSession, tid, rates
                )
                
                if detected_sz is not None:
                    # Handle manifest size mismatch
                    actual_sz = detected_sz
                    # Adjust file size and recalculate chunks
    
    Retry Behavior Example:
        Attempt 1: Downloads 800KB of 1MB chunk → Partial failure
        - Logs: "failed to download setup.exe, byte_range=(0, 1048575) (2 retries left) -- will retry in 5s..."
        - Waits 5 seconds
        
        Attempt 2: Downloads 900KB of 1MB chunk → Partial failure
        - Logs: "failed to download setup.exe, byte_range=(0, 1048575) (1 retries left) -- will retry in 5s..."
        - Waits 5 seconds
        
        Attempt 3: Downloads full 1MB → Success
        - Returns (True, None)
        
        If all retries exhausted:
        - Logs: "failed to download setup.exe, byte_range=(0, 1048575)"
        - Returns (False, None)
    
    Thread Safety Example:
        Thread 1: Downloads bytes 0-1048575
        Thread 2: Downloads bytes 1048576-2097151
        Thread 3: Downloads bytes 2097152-3145727
        
        All three threads:
        - Open same file with open_notruncwrrd
        - Seek to their respective start positions
        - Write to non-overlapping byte ranges
        - Lock only when updating shared sizes/rates dictionaries
        - No file corruption because byte ranges don't overlap
    
    Performance Characteristics:
        - Time Complexity: O(chunk_size) for network transfer + O(retries)
        - Space Complexity: O(1) - streams data, doesn't buffer entire chunk
        - Network Efficiency: Wastes bandwidth on retries (re-downloads entire chunk)
        - Thread Scalability: N threads can download N chunks in parallel safely
    """
    se = start, end
    retries = HTTP_RETRY_COUNT
    downloadSegmentSuccess = False
    actual_sz = None
    
    while not downloadSegmentSuccess and retries >= 0:
        try:
            response = request(downloadSession, href, byte_range=(start, end), stream=True)
            hdr = response.headers['Content-Range'].split()[-1]
            # Parse the actual size from Content-Range: "start-end/total"
            hdr_parts = hdr.split('/')
            if len(hdr_parts) == 2:
                reported_sz = int(hdr_parts[1])
                if reported_sz != sz:
                    with lock:
                        warn("manifest size mismatch for %s: manifest=%d, server=%d - using server size" 
                             % (os.path.basename(path), sz, reported_sz))
                    actual_sz = reported_sz
                    sz = reported_sz
            
            if hdr != '%d-%d/%d' % (start, end, sz):
                with lock:
                    error("chunk request has unexpected Content-Range. "
                          "expected '%d-%d/%d' received '%s'. skipping."
                          % (start, end, sz, hdr))
                return (False, actual_sz)
            
            with open_notruncwrrd(downloading_path) as out:
                out.seek(start)
                assert out.tell() == start
                dlsz = ioloop(tid, path, response, out, sizes, lock, rates)
                
                if dlsz == (end - start) + 1 and out.tell() == end + 1:
                    downloadSegmentSuccess = True
                    return (True, actual_sz)
                else:
                    with lock:
                        sizes[path] += dlsz
                    if retries > 0:
                        warn("failed to download %s, byte_range=%s (%d retries left) -- will retry in %ds..." 
                             % (os.path.basename(path), str(se), retries, HTTP_RETRY_DELAY))
                    else:
                        error("failed to download %s, byte_range=%s" % (os.path.basename(path), str(se)))
                    retries -= 1
                    
        except requests.HTTPError as e:
            with lock:
                error("failed to download %s, byte_range=%s" % (os.path.basename(path), str(se)))
            return (False, actual_sz)
        except Exception as e:
            with lock:
                warn("The unhandled exception was:")
                log_exception('')
                warn("End exception report.")
            raise
    
    return (False, actual_sz)

def download_with_chunk_verification(downloading_path, href, sz, path, sizes, lock, downloadSession, tid, chunk_tree, rates):
    """Download a file using MD5 chunk verification with resume support and corruption detection.
    
    Purpose:
        Download or verify a file using GOG's XML chunk verification data. This function checks
        MD5 hashes for each chunk of the file, downloading only invalid or missing chunks. This
        enables efficient resume functionality and corruption detection, ensuring file integrity
        without re-downloading the entire file.
    
    Args:
        downloading_path: Absolute path to the file being downloaded/verified. File must already
                         exist (created by preallocate_file or previous download attempts).
        href: Full HTTPS URL to the file on GOG's CDN. Used to download invalid chunks.
        sz: Expected total file size in bytes from manifest. Must match chunk_tree total_size
            for verification to proceed.
        path: Display path for logging and progress tracking. Used as key in sizes/rates dicts.
        sizes: Dictionary mapping file paths to remaining bytes to download.
              Shared across threads for progress tracking. Structure: {path: remaining_bytes, ...}
              Valid chunks are subtracted from remaining bytes; invalid chunks are downloaded.
        lock: Threading lock for synchronizing access to shared data structures (sizes, rates).
              Must be acquired before modifying dictionaries or logging.
        downloadSession: Authenticated GOG session for making HTTP requests to download chunks.
        tid: Thread ID for progress tracking and rate monitoring in multi-threaded scenarios.
        chunk_tree: XML ElementTree element containing chunk verification data from GOG.
                   Expected structure:
                   - attrib['name']: Filename
                   - attrib['total_size']: Total file size as string
                   - attrib['chunks']: Number of chunks as string
                   - Child elements: One per chunk with attrib['method'], ['from'], ['to']
                                    and .text containing the hash value
        rates: Dictionary tracking download rates per file and thread.
              Structure: {path: [(tid, (bytes, time_delta)), ...], ...}
    
    Returns:
        tuple: (success: bool, actual_size: int or None)
            - success: True if all chunks are valid (existing or newly downloaded), False otherwise
            - actual_size: Server-reported file size if it differs from manifest sz during
                          chunk download, None if sizes match or verification failed
    
    Behavior:
        XML Validation:
        - Extracts expected_size from chunk_tree.attrib['total_size']
        - Compares expected_size with manifest sz parameter
        - If mismatch, logs error and returns (False, None) immediately
        - Extracts expected chunk count and validates against actual number of child elements
        - If count mismatch, logs error and returns (False, None) immediately
        
        Chunk Processing Loop:
        - Iterates through each chunk element in chunk_tree
        - Validates verification method is 'md5' (only supported method)
        - If non-md5 method found, logs error, marks failure, continues to next chunk
        - Extracts byte range (start, end) and expected MD5 hash from chunk element
        
        Chunk Verification:
        - Opens file with open_notruncwrrd for reading existing content
        - Uses hashstream(out, start, end) to compute MD5 of byte range
        - Compares computed hash with expected hash from chunk_tree
        
        Valid Chunk Handling:
        - If hash matches, chunk is already correct in file
        - Decrements sizes[path] by chunk size: (end - start) + 1
        - This reflects that these bytes don't need downloading
        - No download occurs for valid chunks (resume optimization)
        
        Invalid Chunk Handling:
        - If hash doesn't match, chunk needs to be downloaded
        - Calls download_file_chunk to fetch chunk from CDN
        - download_file_chunk handles retry logic and manifest mismatch detection
        - If download_file_chunk returns actual_size (manifest mismatch), propagates it
        - Updates all_chunks_valid flag with download success status
        
        Success Determination:
        - Returns True only if all chunks are valid or successfully downloaded
        - Any failed chunk download causes overall failure
        - Any non-md5 verification method causes overall failure
        - XML validation failures cause immediate failure
        
        Error Handling:
        - XML validation errors: Logged with details, immediate return (False, None)
        - Unsupported hash method: Logged, chunk marked failed, continues processing
        - Chunk download failures: Propagated from download_file_chunk, affects final result
        - Thread-safe logging using lock for all error messages
        
        Thread Safety:
        - Acquires lock before modifying sizes dictionary
        - Acquires lock before logging errors
        - File reads for hashing are safe (read-only operation)
        - File writes happen in download_file_chunk (thread-safe via byte ranges)
    
    Important Notes:
        - Resume Support: Only downloads invalid chunks, not entire file. Existing valid
          chunks are verified via hash and left untouched.
        - Corruption Detection: MD5 verification catches bit rot, partial writes, or
          transmission errors that occurred in previous download attempts.
        - XML Dependency: Requires GOG's XML verification file. If unavailable, falls back
          to download_without_chunks for simple download.
        - MD5 Only: Currently only supports MD5 verification. Other hash methods (SHA256, etc.)
          are logged as errors and cause failure.
        - Progress Tracking: sizes dictionary tracks remaining bytes. Valid chunks immediately
          decrement this counter, providing accurate progress even without downloads.
        - Manifest Mismatch: If server reports different size during chunk download, actual_size
          is returned for caller to handle (typically adjusts manifest and retries).
    
    Examples:
        # Verify and download invalid chunks of a 100MB file
        chunk_tree = fetch_chunk_tree(game_item.id, file_id)
        success, actual_sz = download_with_chunk_verification(
            downloading_path="/downloading/game/setup.exe",
            href="https://cdn.gog.com/content-system/v2/...",
            sz=104857600,  # 100MB
            path="game/setup.exe",
            sizes={"game/setup.exe": 104857600},  # All bytes remaining
            lock=threading.Lock(),
            downloadSession=session,
            tid=1,
            chunk_tree=chunk_tree,
            rates={}
        )
        # If first 50MB chunks are valid, sizes decrements to 52428800
        # Only invalid chunks in second 50MB are downloaded
        
        # Resume interrupted download
        chunk_tree = fetch_chunk_tree(game_item.id, file_id)
        # File exists with 60MB already downloaded (some valid, some corrupted)
        success, actual_sz = download_with_chunk_verification(
            downloading_path="/downloading/game/installer.bin",
            href="https://cdn.gog.com/...",
            sz=209715200,  # 200MB
            path="game/installer.bin",
            sizes={"game/installer.bin": 209715200},
            lock=threading.Lock(),
            downloadSession=session,
            tid=2,
            chunk_tree=chunk_tree,
            rates={}
        )
        # Valid chunks (even from previous download) are verified and skipped
        # Only corrupted or missing chunks are downloaded
        
        # XML validation failure scenario
        chunk_tree.attrib['total_size'] = "104857600"
        success, actual_sz = download_with_chunk_verification(
            downloading_path="/downloading/game/data.pak",
            href="https://cdn.gog.com/...",
            sz=104857700,  # Doesn't match XML (100 bytes off)
            path="game/data.pak",
            sizes={"game/data.pak": 104857700},
            lock=threading.Lock(),
            downloadSession=session,
            tid=3,
            chunk_tree=chunk_tree,
            rates={}
        )
        # Logs: "XML verification data size does not match manifest size"
        # Returns: (False, None)
    
    Usage in cmd_download:
        This function is called when GOG provides XML chunk verification data:
        
        # Attempt to fetch chunk verification XML
        chunk_tree = None
        try:
            chunk_tree = fetch_chunk_tree(item.id, game_item.id)
        except Exception:
            pass  # Fall back to download_without_chunks
        
        if chunk_tree is not None:
            # Use chunk verification for resume and integrity checking
            download_success, actual_size = download_with_chunk_verification(
                downloading_path, href, sz, path,
                sizes, lock, downloadSession, tid,
                chunk_tree, rates
            )
        else:
            # Fall back to simple download without verification
            download_success, actual_size = download_without_chunks(
                downloading_path, href, 0, sz - 1, sz,
                path, sizes, lock, downloadSession, tid, rates
            )
        
        The chunk verification approach is preferred when available because it:
        - Enables resume from any point (not just file boundaries)
        - Detects corruption early (chunk-by-chunk vs whole file)
        - Reduces bandwidth (only re-downloads invalid chunks)
        - Provides better progress granularity (chunk-level tracking)
    
    XML Structure Example:
        <file name="setup_game_1.0.exe" total_size="104857600" chunks="100">
            <chunk method="md5" from="0" to="1048575">5d41402abc4b2a76b9719d911017c592</chunk>
            <chunk method="md5" from="1048576" to="2097151">7d793037a0760186574b0282f2f435e7</chunk>
            ...
            <chunk method="md5" from="103809024" to="104857599">9e107d9d372bb6826bd81d3542a419d6</chunk>
        </file>
        
        Each chunk:
        - method: Hash algorithm (currently only "md5" supported)
        - from: Starting byte position (0-indexed, inclusive)
        - to: Ending byte position (0-indexed, inclusive)
        - text content: Expected hash value as hex string
    
    Verification Flow Example:
        Initial state: 100MB file, 100 chunks of 1MB each
        
        Chunk 1 (0-1048575): Verify hash
        - Computed: 5d41402abc4b2a76b9719d911017c592
        - Expected: 5d41402abc4b2a76b9719d911017c592
        - Result: VALID → sizes[path] -= 1048576 (no download)
        
        Chunk 2 (1048576-2097151): Verify hash
        - Computed: 00000000000000000000000000000000
        - Expected: 7d793037a0760186574b0282f2f435e7
        - Result: INVALID → download_file_chunk(1048576, 2097151)
        
        Chunk 3 (2097152-3145727): Verify hash
        - Computed: 9e107d9d372bb6826bd81d3542a419d6
        - Expected: 9e107d9d372bb6826bd81d3542a419d6
        - Result: VALID → sizes[path] -= 1048576 (no download)
        
        Final: 99MB valid (no download), 1MB invalid (downloaded)
        Total bandwidth saved: 99MB
    
    Performance Characteristics:
        - Time Complexity: O(n) where n is number of chunks
        - Space Complexity: O(1) for verification (streams file)
        - Hash Computation: ~100MB/s on modern hardware (MD5 is fast)
        - Network Efficiency: Only downloads invalid chunks (optimal bandwidth usage)
        - Resume Capability: Can resume from any chunk boundary (not just file start)
    """
    name = chunk_tree.attrib['name']
    expected_size = int(chunk_tree.attrib['total_size'])
    actual_sz = None
    
    if expected_size != sz:
        with lock:
            error("XML verification data size does not match manifest size for %s. manifest %d, received %d, skipping."
                  % (name, sz, expected_size))
        return (False, None)
    
    expected_no_of_chunks = int(chunk_tree.attrib['chunks'])
    actual_no_of_chunks = len(list(chunk_tree))
    
    if expected_no_of_chunks != actual_no_of_chunks:
        with lock:
            error("XML verification chunk data for %s is not sane skipping." % name)
        return (False, None)
    
    # Process each chunk
    all_chunks_valid = True
    for elem in list(chunk_tree):
        method = elem.attrib["method"]
        if method != "md5":
            error("XML chunk verification method for %s is not md5. skipping." % name)
            all_chunks_valid = False
            continue
        
        start = int(elem.attrib["from"])
        end = int(elem.attrib["to"])
        md5 = elem.text
        
        with open_notruncwrrd(downloading_path) as out:
            valid = hashstream(out, start, end) == md5
            
            if valid:
                with lock:
                    sizes[path] -= (end - start) + 1
            else:
                # Chunk needs to be downloaded
                chunk_success, detected_sz = download_file_chunk(downloading_path, href, start, end, sz, path, sizes, lock, downloadSession, tid, rates)
                if detected_sz is not None:
                    actual_sz = detected_sz
                all_chunks_valid = all_chunks_valid and chunk_success
    
    return (all_chunks_valid, actual_sz)

def download_without_chunks(downloading_path, href, start, end, sz, path, sizes, lock, downloadSession, tid, rates):
    """Download a file without chunk verification using a simple single-request approach with dynamic size adjustment.
    
    Purpose:
        Download an entire file (or remainder of file) in a single HTTP request without MD5 chunk
        verification. This is the fallback method used when GOG doesn't provide XML chunk verification
        data, or for files where chunk verification isn't necessary. Includes automatic size adjustment
        when manifest data doesn't match server-reported file size.
    
    Args:
        downloading_path: Absolute path to the file being downloaded. File must already exist
                         (created by preallocate_file or open_notrunc on first write).
        href: Full HTTPS URL to the file on GOG's CDN. Should be a direct download link
              obtained from the manifest item.
        start: Starting byte position (0-indexed, inclusive) for download.
              Typically 0 for new download, or resume position for partial files.
        end: Ending byte position (0-indexed, inclusive) for download.
            Typically (file_size - 1) for complete file download.
        sz: Expected total file size in bytes from manifest. Used to validate server response
            and for dynamic adjustment if server reports different size.
        path: Display path for logging and progress tracking. Used as key in sizes/rates dicts.
        sizes: Dictionary mapping file paths to remaining bytes to download.
              Shared across threads for progress tracking. Structure: {path: remaining_bytes, ...}
        lock: Threading lock for synchronizing access to shared data structures (sizes, rates).
              Must be acquired before modifying dictionaries or logging.
        downloadSession: Authenticated GOG session for making HTTP requests.
                        Must have valid authentication cookies/tokens.
        tid: Thread ID for progress tracking and rate monitoring in single or multi-threaded scenarios.
        rates: Dictionary tracking download rates per file and thread.
               Structure: {path: [(tid, (bytes, time_delta)), ...], ...}
    
    Returns:
        tuple: (success: bool, actual_size: int or None)
            - success: True if file downloaded completely, False otherwise
            - actual_size: Server-reported file size if it differs from manifest sz,
                          None if sizes match or download failed
    
    Behavior:
        File Opening:
        - Opens file with open_notrunc (no truncation on open)
        - Preserves existing file content for resume scenarios
        - File remains open for entire download duration
        
        Request Initiation:
        - Makes HTTP GET request with Range header: byte_range=(start, end)
        - Streams response to avoid loading entire file into memory
        - Uses authenticated downloadSession with GOG credentials
        
        Content-Range Header Parsing:
        - Parses "Content-Range" header: "bytes start-end/total_size"
        - Extracts server-reported file size from header
        - Compares with manifest sz parameter
        
        Dynamic Size Adjustment:
        - If server reports different size than manifest:
          * Logs warning with both sizes
          * Updates sz to use server-reported size
          * Recalculates end position: reported_sz - 1
          * Updates sizes[path] by size difference
          * Resizes file using out.truncate(sz) to match new size
          * Returns actual_size for caller to handle
        - This handles cases where GOG updates files but manifest is stale
        - Unlike download_file_chunk, this function can adjust file size on the fly
        
        Header Validation:
        - Verifies Content-Range matches expected format exactly
        - Expected: "start-end/sz" (e.g., "0-104857599/104857600")
        - If mismatch, logs error and returns (False, actual_sz)
        - Prevents corrupted downloads from malformed server responses
        
        File Writing:
        - Seeks to start position in file
        - Asserts file pointer is at correct position (safety check)
        - Calls ioloop to stream response content to file
        - ioloop handles chunked reading and progress tracking
        
        Download Verification:
        - Checks if downloaded size matches expected: (end - start) + 1
        - Verifies file pointer ended at expected position: end + 1
        - Both conditions must be true for success
        
        Retry Logic:
        - Retries up to HTTP_RETRY_COUNT times on partial downloads
        - Sleeps HTTP_RETRY_DELAY seconds between retries
        - Logs retry attempts with remaining count
        - Updates sizes[path] to reflect partial progress
        - Each retry attempts full remaining download (not just missing bytes)
        
        Error Handling:
        - HTTPError: Logs error, returns (False, actual_sz) immediately (no retry)
        - Other exceptions: Logs detailed traceback and re-raises (fatal)
        - Thread-safe logging using lock
        
        Thread Safety:
        - Acquires lock before modifying sizes dictionary
        - Acquires lock before logging
        - Single file write operation (no parallel chunk writes)
    
    Important Notes:
        - No Chunk Verification: Unlike download_with_chunk_verification, this function doesn't
          verify file integrity via MD5 hashes. Corruption detection relies on complete download.
        - Single Request: Downloads entire file (or remainder) in one request. No parallel
          chunk downloading possible with this approach.
        - Dynamic Size Adjustment: Unique feature - can adjust file size mid-download if server
          reports different size. download_file_chunk doesn't do this (just returns actual_size).
        - Resume Support: Can resume from any byte position, but without integrity verification
          of already-downloaded content.
        - Fallback Method: Used when XML chunk verification unavailable or for small files where
          chunk verification overhead isn't justified.
        - File Truncation: Uses out.truncate(sz) to adjust file size. This can grow or shrink
          the file based on server-reported size.
    
    Examples:
        # Download complete file without verification
        success, actual_sz = download_without_chunks(
            downloading_path="/downloading/game/manual.pdf",
            href="https://cdn.gog.com/content-system/v2/...",
            start=0,
            end=1048575,  # 1MB file (bytes 0-1048575)
            sz=1048576,  # 1MB
            path="game/manual.pdf",
            sizes={"game/manual.pdf": 1048576},
            lock=threading.Lock(),
            downloadSession=session,
            tid=1,
            rates={}
        )
        # Downloads entire file in single request
        # Returns: (True, None) if successful
        
        # Download with size mismatch adjustment
        success, actual_sz = download_without_chunks(
            downloading_path="/downloading/game/setup.exe",
            href="https://cdn.gog.com/...",
            start=0,
            end=104857599,  # Expecting 100MB
            sz=104857600,  # Manifest says 100MB
            path="game/setup.exe",
            sizes={"game/setup.exe": 104857600},
            lock=threading.Lock(),
            downloadSession=session,
            tid=1,
            rates={}
        )
        # If server reports 104858000 (400 bytes larger):
        # - Logs: "manifest size mismatch: manifest=104857600, server=104858000 - adjusting"
        # - Updates end to 104857999
        # - Adjusts sizes[path] += 400
        # - Truncates file to 104858000 bytes
        # - Returns: (True, 104858000)
        
        # Resume interrupted download
        success, actual_sz = download_without_chunks(
            downloading_path="/downloading/game/data.bin",
            href="https://cdn.gog.com/...",
            start=52428800,  # Resume from 50MB
            end=104857599,   # Download to 100MB
            sz=104857600,    # Total 100MB
            path="game/data.bin",
            sizes={"game/data.bin": 52428800},  # 50MB remaining
            lock=threading.Lock(),
            downloadSession=session,
            tid=1,
            rates={}
        )
        # Downloads second half of file (50MB-100MB)
        # First half (0-50MB) already exists from previous download
    
    Usage in cmd_download:
        This function is called when chunk verification is unavailable:
        
        # Try to get chunk verification data
        chunk_tree = None
        try:
            chunk_tree = fetch_chunk_tree(item.id, game_item.id)
        except Exception:
            pass  # XML not available
        
        if chunk_tree is not None:
            # Preferred: Use chunk verification
            download_success, actual_size = download_with_chunk_verification(...)
        else:
            # Fallback: Simple download without verification
            download_success, actual_size = download_without_chunks(
                downloading_path, href, 0, sz - 1, sz,
                path, sizes, lock, downloadSession, tid, rates
            )
        
        Chunk verification is preferred but not always available:
        - Some files don't have XML verification data
        - XML fetch may fail due to network issues
        - Small files may not justify verification overhead
    
    Comparison with download_with_chunk_verification:
        download_without_chunks:
        - Single HTTP request for entire file
        - No integrity verification during download
        - Cannot detect partial corruption
        - Simpler, less overhead
        - Falls back to re-downloading entire file on any failure
        - Can dynamically adjust file size
        
        download_with_chunk_verification:
        - Multiple HTTP requests (one per invalid chunk)
        - MD5 verification for each chunk
        - Detects and repairs partial corruption
        - More complex, higher overhead
        - Only re-downloads invalid chunks
        - Cannot adjust file size (must match XML)
    
    Retry Behavior Example:
        Attempt 1: Downloads 80MB of 100MB file → Partial failure
        - Logs: "failed to download setup.exe, byte_range=(0, 104857599) (2 retries left) -- will retry in 5s..."
        - Updates sizes[path] += 83886080 (80MB progress tracked)
        - Sleeps 5 seconds
        
        Attempt 2: Downloads 90MB of 100MB file → Partial failure
        - Logs: "failed to download setup.exe, byte_range=(0, 104857599) (1 retries left) -- will retry in 5s..."
        - Updates sizes[path] += 94371840 (90MB progress tracked)
        - Sleeps 5 seconds
        
        Attempt 3: Downloads full 100MB → Success
        - Returns: (True, None)
        
        If all retries exhausted:
        - Logs: "failed to download setup.exe, byte_range=(0, 104857599)"
        - Returns: (False, None)
        
        Note: Each retry downloads from start, not incrementally. Partial progress
        is tracked but not used for resume (starts over each retry).
    
    Performance Characteristics:
        - Time Complexity: O(file_size) for single large transfer
        - Space Complexity: O(1) - streams data in 4KB chunks
        - Network Efficiency: Wastes bandwidth on retry (re-downloads entire file)
        - Resume Capability: Can resume from any byte position
        - Overhead: Minimal compared to chunk verification
        - Ideal for: Small files, files without XML verification, or when simplicity preferred
    
    When to Use:
        - XML chunk verification data unavailable
        - Small files (< 10MB) where chunk overhead not justified
        - Files rarely interrupted (extras, small downloads)
        - Bandwidth plentiful and reliability high (less need for verification)
        
    When to Avoid:
        - Large files (> 100MB) with available chunk verification
        - Unreliable connections (chunk verification enables partial resume)
        - Critical files where integrity verification important
    """
    se = start, end
    retries = HTTP_RETRY_COUNT
    downloadSuccess = False
    actual_sz = None
    
    with open_notrunc(downloading_path) as out:
        while not downloadSuccess and retries >= 0:
            try:
                response = request(downloadSession, href, byte_range=(start, end), stream=True)
                hdr = response.headers['Content-Range'].split()[-1]
                
                # Parse the actual size from Content-Range: "start-end/total"
                hdr_parts = hdr.split('/')
                if len(hdr_parts) == 2:
                    reported_sz = int(hdr_parts[1])
                    if reported_sz != sz:
                        with lock:
                            warn("manifest size mismatch for %s: manifest=%d, server=%d - adjusting" 
                                 % (os.path.basename(path), sz, reported_sz))
                        actual_sz = reported_sz
                        # Adjust end position and size tracking
                        size_diff = reported_sz - sz
                        end = reported_sz - 1
                        sz = reported_sz
                        with lock:
                            sizes[path] += size_diff
                        # Resize the file
                        out.truncate(sz)
                
                if hdr != '%d-%d/%d' % (start, end, sz):
                    with lock:
                        error("chunk request has unexpected Content-Range. "
                              "expected '%d-%d/%d' received '%s'. skipping."
                              % (start, end, sz, hdr))
                    return (False, actual_sz)
                
                out.seek(start)
                assert out.tell() == start
                dlsz = ioloop(tid, path, response, out, sizes, lock, rates)
                
                if dlsz == (end - start) + 1 and out.tell() == end + 1:
                    downloadSuccess = True
                    return (True, actual_sz)
                else:
                    with lock:
                        sizes[path] += dlsz
                        if retries > 0:
                            warn("failed to download %s, byte_range=%s (%d retries left) -- will retry in %ds..." 
                                 % (os.path.basename(path), str(se), retries, HTTP_RETRY_DELAY))
                            time.sleep(HTTP_RETRY_DELAY)
                        else:
                            error("failed to download %s, byte_range=%s" % (os.path.basename(path), str(se)))
                    retries -= 1
                    
            except requests.HTTPError as e:
                error("failed to download %s, byte_range=%s" % (os.path.basename(path), str(se)))
                return (False, actual_sz)
            except Exception as e:
                with lock:
                    warn("The unhandled exception was:")
                    log_exception('')
                    warn("End exception report.")
                raise
    
    return (False, actual_sz)

def killresponse(response):
    """Close a response object (used for timeout handling)."""
    response.close()

def ioloop(tid, path, response, out, sizes, lock, rates):
    """Download content from HTTP response and write to file, tracking progress and bandwidth with timeout protection.
    
    Purpose:
        Stream HTTP response content to disk in fixed-size chunks while maintaining progress tracking,
        bandwidth monitoring, and timeout protection. This is the core data transfer function used by
        all download methods (download_file_chunk, download_without_chunks) to perform the actual
        network-to-disk I/O operation. Handles transient network issues gracefully and provides
        real-time progress updates for UI display.
    
    Args:
        tid: Thread ID for identifying which thread is performing the download.
             Used to associate bandwidth measurements with specific threads in multi-threaded scenarios.
        path: Display path for the file being downloaded. Used as dictionary key for progress tracking
              and in error messages. Typically the final destination path or user-friendly name.
        response: HTTP response object from requests library with streaming enabled.
                 Must have iter_content() method for chunked reading. Should be created with
                 stream=True parameter to enable efficient memory usage.
        out: Output file handle opened for binary writing. File pointer must be positioned at
             the correct starting byte (via out.seek() before calling this function).
        sizes: Dictionary mapping file paths to remaining bytes to download.
              Structure: {path: remaining_bytes, ...}
              Updated after each chunk write to reflect progress. Shared across threads.
        lock: Threading lock for synchronizing access to shared data structures (sizes, rates).
              Must be acquired before modifying sizes or rates dictionaries to prevent race conditions.
        rates: Dictionary tracking download bandwidth per file and thread.
               Structure: {path: [(tid, (bytes, time_delta)), ...], ...}
               Each entry is a tuple of (thread_id, (bytes_downloaded, time_elapsed)).
               Used by display_progress_lines to calculate and display download speeds.
    
    Returns:
        int: Total number of bytes successfully downloaded and written to file.
             This value may be less than expected if connection errors occur.
             Caller should compare with expected size to determine success.
    
    Behavior:
        Initialization:
        - Records start time for bandwidth calculation: t0 = time.time()
        - Initializes downloaded byte counter: dlsz = 0
        - Creates timeout timer to prevent hung connections: HTTP_TIMEOUT seconds
        - Starts initial timer before beginning chunk iteration
        
        Chunked Reading Loop:
        - Iterates through response content in 4KB chunks (4*1024 bytes)
        - Each iteration cancels previous timeout timer (connection still alive)
        - Checks if chunk is non-empty (empty chunks signal end or keep-alive)
        
        Chunk Processing (for non-empty chunks):
        - Records current time for bandwidth calculation
        - Writes chunk to file at current file pointer position
        - Calculates chunk size and time delta since last chunk
        - Accumulates total downloaded size: dlsz += chunk_size
        - Thread-safely updates shared progress tracking:
          * sizes[path] -= chunk_size (decrement remaining bytes)
          * rates[path].append((tid, (chunk_size, time_delta))) (record bandwidth)
        - Starts new timeout timer for next chunk (reset timeout protection)
        
        Timeout Protection:
        - Uses threading.Timer with HTTP_TIMEOUT delay to detect stalled connections
        - Timer calls killresponse(response) if triggered, closing the response
        - Timer is cancelled after each successful chunk read (connection alive)
        - Timer is restarted before reading next chunk (renewed protection)
        - Final timer cancelled at end regardless of success or failure
        
        Error Handling:
        - ConnectionError/ProtocolError: Network connection issues during transfer
          * Logs user-friendly error message with file path
          * Returns partial download size (dlsz) for caller to handle
          * Does not raise exception (allows graceful degradation)
        - SSLError: SSL/TLS issues during encrypted transfer
          * Logs SSL-specific error message with file path
          * Returns partial download size (dlsz) for caller to handle
          * Does not raise exception (allows graceful degradation)
        - Other exceptions: Propagate to caller (fatal errors)
        
        Cleanup:
        - Cancels timeout timer before returning (prevents timer firing after function exit)
        - Returns total downloaded bytes for verification by caller
        - File handle remains open (caller's responsibility to close)
        - Response object may be closed by timeout or exception
        
        Thread Safety:
        - File writes are safe because each thread writes to different byte ranges
          (file pointer positioned by caller before calling ioloop)
        - Dictionary updates are protected by lock acquisition
        - Multiple threads can call ioloop simultaneously for same file safely
    
    Important Notes:
        - Chunk Size: 4KB (4*1024 bytes) balances memory usage, progress granularity, and I/O efficiency
        - Streaming: Response must be created with stream=True to avoid loading entire file into memory
        - Timeout Reset: Timer is reset after EVERY chunk, so slow but continuous downloads don't timeout
        - Partial Downloads: Returns partial byte count on error, enabling retry logic in caller
        - Progress Granularity: Progress updates every 4KB, providing smooth progress bar updates
        - Bandwidth Tracking: Per-chunk timing enables accurate real-time speed calculation
        - Error Recovery: Network errors are non-fatal, allowing caller to implement retry logic
    
    Examples:
        # Basic usage in single-threaded download
        with open("/downloading/game/setup.exe", "r+b") as out:
            out.seek(0)  # Start at beginning
            response = downloadSession.get(url, stream=True)
            dlsz = ioloop(
                tid=1,
                path="game/setup.exe",
                response=response,
                out=out,
                sizes={"game/setup.exe": 104857600},  # 100MB remaining
                lock=threading.Lock(),
                rates={}
            )
            # dlsz should be 104857600 if successful
        
        # Multi-threaded chunk download scenario
        def download_chunk_worker(tid, start, end):
            with open_notruncwrrd(downloading_path) as out:
                out.seek(start)
                response = downloadSession.get(
                    url,
                    headers={"Range": f"bytes={start}-{end}"},
                    stream=True
                )
                dlsz = ioloop(
                    tid=tid,
                    path=path,
                    response=response,
                    out=out,
                    sizes=sizes,  # Shared dict
                    lock=lock,    # Shared lock
                    rates=rates   # Shared dict
                )
                return dlsz
        
        # Thread 1: Downloads bytes 0-1048575 (first 1MB)
        # Thread 2: Downloads bytes 1048576-2097151 (second 1MB)
        # Thread 3: Downloads bytes 2097152-3145727 (third 1MB)
        # All threads safely update sizes and rates via lock
        
        # Timeout protection example
        # If no data received for HTTP_TIMEOUT (60s default):
        # - Timer fires, calls killresponse(response)
        # - response.iter_content() raises ConnectionError
        # - ioloop catches error, logs message, returns partial dlsz
        # - Caller detects partial download, retries chunk
    
    Usage in Download Functions:
        This function is called by all download methods to perform actual data transfer:
        
        # In download_file_chunk
        with open_notruncwrrd(downloading_path) as out:
            out.seek(start)
            response = request(downloadSession, href, byte_range=(start, end), stream=True)
            dlsz = ioloop(tid, path, response, out, sizes, lock, rates)
            # Verify dlsz == expected chunk size
        
        # In download_without_chunks
        with open_notrunc(downloading_path) as out:
            out.seek(start)
            response = request(downloadSession, href, byte_range=(start, end), stream=True)
            dlsz = ioloop(tid, path, response, out, sizes, lock, rates)
            # Verify dlsz == expected file size
        
        The ioloop function abstracts the complexity of:
        - Chunked reading with proper chunk size
        - Progress tracking with thread safety
        - Bandwidth monitoring with per-chunk timing
        - Timeout protection with timer management
        - Error handling with graceful degradation
    
    Progress Tracking Mechanism:
        sizes dictionary provides real-time remaining bytes:
        
        Initial state: sizes["game.exe"] = 104857600  # 100MB
        
        After chunk 1 (4KB): sizes["game.exe"] = 104853504  # 99.996MB
        After chunk 2 (4KB): sizes["game.exe"] = 104849408  # 99.992MB
        ...
        After final chunk:   sizes["game.exe"] = 0          # Complete
        
        display_progress_lines reads sizes dictionary to show:
        "game.exe: 45.2 MB / 100 MB (45.2%) @ 5.2 MB/s"
    
    Bandwidth Tracking Mechanism:
        rates dictionary captures per-chunk timing for speed calculation:
        
        rates["game.exe"] = [
            (1, (4096, 0.001)),    # Thread 1: 4KB in 1ms = 4 MB/s
            (1, (4096, 0.0008)),   # Thread 1: 4KB in 0.8ms = 5 MB/s
            (2, (4096, 0.0012)),   # Thread 2: 4KB in 1.2ms = 3.3 MB/s
            ...
        ]
        
        display_progress_lines calculates recent average:
        - Sums bytes from last N entries
        - Sums time deltas from last N entries
        - Speed = total_bytes / total_time
    
    Timeout Behavior Example:
        Normal operation (data flowing):
        - Start timer (60s)
        - Receive chunk 1 after 0.1s → Cancel timer, write chunk, start new timer
        - Receive chunk 2 after 0.1s → Cancel timer, write chunk, start new timer
        - Receive chunk 3 after 0.1s → Cancel timer, write chunk, start new timer
        - Connection never times out (timer reset before expiration)
        
        Stalled connection:
        - Start timer (60s)
        - Receive chunk 1 after 0.1s → Cancel timer, write chunk, start new timer
        - Receive chunk 2 after 0.1s → Cancel timer, write chunk, start new timer
        - No more data received...
        - Timer expires after 60s → killresponse(response) called
        - response.iter_content() raises ConnectionError
        - Error caught, partial download size returned
        - Caller can retry from last successful position
    
    Performance Characteristics:
        - Time Complexity: O(file_size / chunk_size) = O(file_size / 4096) iterations
        - Space Complexity: O(1) - single 4KB chunk buffer, no accumulation
        - I/O Pattern: Sequential writes at fixed 4KB granularity (optimal for most filesystems)
        - Memory Usage: Minimal - only one 4KB chunk in memory at a time
        - CPU Overhead: Negligible - simple write operations and arithmetic
        - Lock Contention: Minimal - lock held briefly (dictionary update only, not I/O)
        - Progress Update Frequency: Every 4KB (smooth progress, ~25,600 updates per 100MB)
        - Bandwidth Calculation: Per-chunk timing enables 0.1-1.0 second average speeds
    
    When This Function Is Critical:
        - Large file downloads: Streaming prevents memory exhaustion
        - Multi-threaded downloads: Lock-based progress tracking prevents race conditions
        - Slow/unreliable connections: Timeout protection prevents hung downloads
        - Real-time UI updates: Per-chunk progress enables responsive progress bars
        - Bandwidth monitoring: Per-chunk timing enables accurate speed display
        - Partial download recovery: Returns partial size for intelligent retry logic
    """
    sz, t0 = True, time.time()
    dlsz = 0
    responseTimer = threading.Timer(HTTP_TIMEOUT, killresponse, [response])
    responseTimer.start()
    
    try:
        for chunk in response.iter_content(chunk_size=4*1024):
            responseTimer.cancel()
            if chunk:
                t = time.time()
                out.write(chunk)
                sz, dt, t0 = len(chunk), t - t0, t
                dlsz += sz
                with lock:
                    sizes[path] -= sz
                    rates.setdefault(path, []).append((tid, (sz, dt)))
            responseTimer = threading.Timer(HTTP_TIMEOUT, killresponse, [response])
            responseTimer.start()
    except (requests.exceptions.ConnectionError, requests.packages.urllib3.exceptions.ProtocolError) as e:
        error("server response issue while downloading content for %s" % path)
    except (requests.exceptions.SSLError) as e:
        error("SSL issue while downloading content for %s" % path)
        
    responseTimer.cancel()
    return dlsz

def display_progress_lines(progress_lines, last_line_count):
    """Display progress lines with clean terminal rewriting using ANSI escape sequences.
    
    Args:
        progress_lines: List of strings to display, one per line. Each string should be a formatted
                       progress line containing file name, download percentage, size, and speed.
                       Example: "game.exe: 45.2 MB / 100 MB (45.2%) @ 5.2 MB/s"
                       Empty list causes immediate return (no display).
        last_line_count: Single-element list containing the count of previously displayed lines.
                        Must be mutable (list) to persist state between calls. Structure: [count]
                        Used to determine how many lines to overwrite on next update.
                        Initialize with [0] for first call.
    """
    if not progress_lines:
        return
    
    # Move cursor up to overwrite previous lines
    if last_line_count[0] > 0:
        sys.stdout.write('\033[%dA' % last_line_count[0])
    
    # Print all progress lines with proper clearing
    for line in progress_lines:
        sys.stdout.write('\r' + line.ljust(120) + '\n')
    
    # Move cursor back up to keep progress at same position
    sys.stdout.write('\033[%dA' % len(progress_lines))
    
    last_line_count[0] = len(progress_lines)
    sys.stdout.flush()

def _recover_provisional_leftover_files(items, provisionaldir, savedir):
    """Recover and validate leftover provisional files from interrupted downloads.
    
    Purpose:
        Scan the provisional directory for files left behind by previous interrupted download
        sessions, validate their integrity (size and MD5 hash), and move validated files to
        their final destination. This enables automatic recovery of completed downloads that
        weren't moved due to interruption, avoiding unnecessary re-downloads.
    
    Args:
        items: List of game items to check for leftover files. Each item should have:
               - folder_name or title: Directory name for the game
               - downloads: List of download items with name, size, md5 attributes
               - extras: List of extra items with name, size, md5 attributes
        provisionaldir: Absolute path to provisional directory containing leftover files.
                       Structure: provisionaldir/game_folder/filename
        savedir: Absolute path to final save directory where validated files will be moved.
                Structure: savedir/game_folder/filename
    
    Returns:
        int: Count of successfully validated and moved files. Returns 0 if no files recovered.
    
    Behavior:
        Directory Scanning:
        - Iterates through each game item's provisional subdirectory
        - Lists all files in item_provisionaldir if directory exists
        - Skips items with no provisional directory
        
        File Validation (per file):
        - Checks if file already exists at final destination
          * If exists: Logs warning, skips move (avoids overwrite)
        - Retrieves expected size and MD5 from game item metadata
          * Searches item.downloads + item.extras for matching filename
        - Validates file size matches expected size
          * If mismatch: Logs warning with actual vs expected, skips move
        - Validates MD5 hash if available in metadata
          * Computes actual MD5 using hashfile()
          * If mismatch: Logs warning, skips move
        
        File Recovery:
        - Creates final destination directory if needed
        - Moves validated file from provisional to final location using shutil.move()
        - Increments recovery counter for each successful move
        - Logs each successful move with source and destination paths
        
        Logging:
        - Info: Each successful file move
        - Warn: Size mismatches, MD5 mismatches, existing files at destination
    
    Important Notes:
        - Non-Destructive: Never overwrites existing files at destination
        - Validation Required: Files must pass size check AND MD5 check (if available) to be moved
        - Metadata Dependency: Requires game item metadata to have accurate size/MD5
        - Partial Recovery: Invalid files remain in provisional directory for manual inspection
        - Directory Creation: Automatically creates destination directories as needed
        - No Cleanup: Does not delete provisional directory or invalid files
    
    Examples:
        # Recover leftover files from previous interrupted download
        items = load_manifest()
        provisionaldir = "/downloads/.provisional"
        savedir = "/downloads"
        
        recovered = _recover_provisional_leftover_files(items, provisionaldir, savedir)
        # If 3 valid files found:
        # - Validates each file (size + MD5)
        # - Moves to savedir/game_name/filename
        # - Returns: 3
        
        # File with size mismatch (corrupted download)
        # Provisional: setup.exe (100MB actual, 105MB expected)
        # Result: Logs warning, file stays in provisional, not moved
        
        # File with MD5 mismatch (corrupted or modified)
        # Provisional: data.bin (correct size, wrong MD5)
        # Result: Logs warning, file stays in provisional, not moved
    
    Usage in cmd_download:
        Called early in download workflow to recover previous work:
        
        # Before starting new downloads
        if not dryrun and os.path.exists(provisionaldir):
            info("checking for leftover files in provisional directory...")
            leftover_count = _recover_provisional_leftover_files(
                items, provisionaldir, savedir
            )
            if leftover_count > 0:
                info("moved %d validated leftover file(s) from provisional to final location" % leftover_count)
        
        This prevents re-downloading files that were already completed but not moved
        due to interruption (e.g., Ctrl+C, crash, power loss).
    
    Validation Flow Example:
        Provisional directory contains: game/setup.exe (100MB)
        
        Step 1: Check if savedir/game/setup.exe exists
        - Not found → Continue validation
        
        Step 2: Find metadata in item.downloads
        - Found: name="setup.exe", size=104857600, md5="abc123..."
        
        Step 3: Validate size
        - Actual: 104857600 bytes
        - Expected: 104857600 bytes
        - Result: PASS
        
        Step 4: Validate MD5
        - Compute: hashfile("provisional/game/setup.exe")
        - Actual: "abc123..."
        - Expected: "abc123..."
        - Result: PASS
        
        Step 5: Move file
        - Create: savedir/game/ (if needed)
        - Move: provisional/game/setup.exe → savedir/game/setup.exe
        - Log: "moving validated leftover file '...' to '...'"
        - Return: 1 (file recovered)
    
    Performance Characteristics:
        - Time Complexity: O(items × files × downloads) for metadata lookup
        - Space Complexity: O(1) - processes one file at a time
        - I/O Operations: File size check (stat), MD5 computation (full file read), move (rename)
        - MD5 Computation: ~100-200 MB/s on typical hardware (full file hash)
        - Network: None (local file operations only)
    
    Why This Function Exists:
        - Prevents Data Loss: Recovers completed downloads from interrupted sessions
        - Saves Bandwidth: Avoids re-downloading already completed files
        - User-Friendly: Automatic recovery without manual intervention
        - Safety: Validates integrity before moving (prevents corrupted file usage)
        - Robustness: Handles crashes, Ctrl+C, power loss gracefully
    """
    leftover_count = 0
    for item in items:
        try:
            folder_name = item.folder_name
        except AttributeError:
            folder_name = item.title
        
        item_provisionaldir = os.path.join(provisionaldir, folder_name)
        item_finaldir = os.path.join(savedir, folder_name)
        
        if os.path.exists(item_provisionaldir):
            for filename in os.listdir(item_provisionaldir):
                provisional_file = os.path.join(item_provisionaldir, filename)
                final_file = os.path.join(item_finaldir, filename)
                
                if os.path.isfile(provisional_file):
                    if os.path.exists(final_file):
                        warn("leftover provisional file '%s' already exists at destination - skipping move" % filename)
                        continue
                    
                    # Validate the provisional file is complete
                    file_size = os.path.getsize(provisional_file)
                    expected_size = None
                    file_md5 = None
                    
                    # Find the corresponding game item to get expected size and MD5
                    for game_item in item.downloads + item.extras:
                        if game_item.name == filename:
                            expected_size = game_item.size
                            try:
                                file_md5 = game_item.md5
                            except AttributeError:
                                file_md5 = None
                            break
                    
                    # Verify file size
                    if expected_size and file_size != expected_size:
                        warn("leftover provisional file '%s' has incorrect size (%d expected %d) - not moving" % (filename, file_size, expected_size))
                        continue
                    
                    # Verify MD5 if available
                    if file_md5:
                        actual_md5 = hashfile(provisional_file)
                        if actual_md5 != file_md5:
                            warn("leftover provisional file '%s' has incorrect MD5 - not moving" % filename)
                            continue
                    
                    # File validated, move to final location
                    info("moving validated leftover file '%s' to '%s'" % (provisional_file, final_file))
                    if not os.path.exists(item_finaldir):
                        os.makedirs(item_finaldir)
                    shutil.move(provisional_file, final_file)
                    leftover_count += 1
    
    return leftover_count

def cmd_download(savedir, skipextras,skipids, dryrun, ids,os_list, lang_list,skipgalaxy,skipstandalone,skipshared, skipfiles,covers,backgrounds,skippreallocation,clean_old_images,downloadLimit = None):
    sizes, rates, errors = {}, {}, {}
    work = Queue()  # build a list of work items
    work_provisional = Queue()  # build a list of work items for provisional

    if not dryrun:
        downloadSession = makeGOGSession()
        renew_token(downloadSession)  # Check and renew token if needed before downloading
    
    items = load_manifest()
    all_items = items
    work_dict = dict()
    provisional_dict = dict()

    info("Loaded {} games from manifest".format(len(items)))

    # Filter games by IDs
    game_filter = GameFilter(ids=ids if ids else [], skipids=skipids if skipids else [])
    items = filter_games_by_id(items, game_filter)

    if skipfiles:
        formattedSkipFiles = "'" + "', '".join(skipfiles) + "'"
        info("skipping files that match: {%s}" % formattedSkipFiles)

    handle_game_renames(savedir,items,dryrun)

    #writable_items = load_manifest() #Load unchanged item list, so we don't save the filtering stuff.
    all_items_by_id = {}
    for item in all_items:
        all_items_by_id[item.id] = item
        
 
    
    all_items_by_title = {}    

    # make convenient dict with title/dirname as key
    for item in all_items:
        try:
            _ = item.folder_name 
        except AttributeError:
            item.folder_name = item.title
        all_items_by_title[item.folder_name] = item
        

    downloadingdir = os.path.join(savedir, DOWNLOADING_DIR_NAME)    
    provisionaldir = os.path.join(downloadingdir,PROVISIONAL_DIR_NAME )
    orphandir =  os.path.join(savedir, ORPHAN_DIR_NAME)  
    
    # Process any leftover files in provisional directory from previous interrupted downloads
    if not dryrun and os.path.exists(provisionaldir):
        info("checking for leftover files in provisional directory...")
        leftover_count = _recover_provisional_leftover_files(items, provisionaldir, savedir)
        if leftover_count > 0:
            info("moved %d validated leftover file(s) from provisional to final location" % leftover_count)
    
    # Clean up temporary directories
    clean_up_temp_directory(downloadingdir, all_items_by_title, dryrun, skip_subdir=PROVISIONAL_DIR_NAME)
    clean_up_temp_directory(provisionaldir, all_items_by_title, dryrun)
                                        
                                        
        
    for item in items:
        try:
            _ = item.folder_name 
        except AttributeError:
            item.folder_name = item.title
            

    # Find all items to be downloaded and push into work queue
    for item in sorted(items, key=lambda g: g.folder_name):
        info("{%s}" % item.folder_name)
        item_homedir = os.path.join(savedir, item.folder_name)
        item_downloaddir = os.path.join(downloadingdir, item.folder_name)
        item_provisionaldir =os.path.join(provisionaldir, item.folder_name)
        item_orphandir = os.path.join(orphandir,item.folder_name)
        if not dryrun:
            if not os.path.isdir(item_homedir):
                os.makedirs(item_homedir)
                
        try:
            _ = item.galaxyDownloads
        except AttributeError:
            item.galaxyDownloads = []
            
        try:
            a = item.sharedDownloads
        except AttributeError:
            item.sharedDownloads = []

        filtered_extras = item.extras
        filtered_downloads = item.downloads
        filtered_galaxyDownloads = item.galaxyDownloads
        filtered_sharedDownloads = item.sharedDownloads

        if skipextras:
            filtered_extras = []
            
        if skipstandalone:    
            filtered_downloads = []
            
        if skipgalaxy: 
            filtered_galaxyDownloads = []
            
        if skipshared:
            filtered_sharedDownloads = []      
                    
        # Filter all download types by OS and language
        filtered_downloads = filter_downloads_by_os_and_lang(filtered_downloads, os_list, lang_list)
        filtered_galaxyDownloads = filter_downloads_by_os_and_lang(filtered_galaxyDownloads, os_list, lang_list)
        filtered_sharedDownloads = filter_downloads_by_os_and_lang(filtered_sharedDownloads, os_list, lang_list)

        # Generate and save game info and serial files
        if not dryrun:
            write_game_info_file(item_homedir, item, filtered_downloads, filtered_galaxyDownloads, filtered_sharedDownloads, filtered_extras)
            write_game_serial_file(item_homedir, item)

        # Download images (covers and backgrounds)
        if not dryrun:
            images_dir_name = os.path.join(item_homedir, IMAGES_DIR_NAME)
            image_orphandir = os.path.join(item_orphandir, IMAGES_DIR_NAME)

            if not os.path.exists(images_dir_name):
                os.makedirs(images_dir_name)
            try:
                if len(item.bg_urls) != 0 and backgrounds:
                    images_old_bg_url_dir_name = os.path.join(images_dir_name, "bg_url")
                    modified_image_orphandir = image_orphandir  
                    if (platform.system() == "Windows" and sys.version_info[0] < 3):
                        images_old_bg_url_dir_name = uLongPathPrefix + os.path.abspath(images_old_bg_url_dir_name)
                        modified_image_orphandir = uLongPathPrefix + os.path.abspath(modified_image_orphandir)
                    if os.path.exists(images_old_bg_url_dir_name):
                        try:
                            if clean_old_images:
                                if not os.path.exists(modified_image_orphandir):
                                    os.makedirs(modified_image_orphandir)
                                move_with_increment_on_clash(images_old_bg_url_dir_name, modified_image_orphandir)
                            else:
                                shutil.rmtree(images_old_bg_url_dir_name)
                        except Exception as e:
                            error("Could not delete potential old bg_url files, aborting update attempt. Please make sure folder and files are writeable and that nothing is accessing the !image folder")
                            raise
                    try:
                        download_image_from_item_keys(item, "bg_urls", images_dir_name, image_orphandir, clean_old_images, downloadSession)
                    except KeyboardInterrupt:
                        warn("Interrupted during download of background image(s)")
                        raise
                    except Exception:
                        warn("Could not download background image")
                    
            except AttributeError:
                if item.bg_url != '' and backgrounds:
                    try:
                        download_image_from_item_key(item, "bg_url", images_dir_name, image_orphandir, clean_old_images, downloadSession)
                    except KeyboardInterrupt:
                        warn("Interrupted during download of background image")
                        raise
                    except Exception:
                        warn("Could not download background image")
                
            if item.image_url != '' and covers:
                try:
                    download_image_from_item_key(item, "image_url", images_dir_name, image_orphandir, clean_old_images, downloadSession)
                except KeyboardInterrupt:
                    warn("Interrupted during download of cover image")
                    raise
                except Exception:
                    warn("Could not download cover image")

        # Populate queue with all files to be downloaded
        for game_item in filtered_downloads + filtered_galaxyDownloads + filtered_sharedDownloads + filtered_extras:
            if game_item.name is None:
                continue  # no game name, usually due to 404 during file fetch

            try:
                _ = game_item.force_change
            except AttributeError:
                game_item.force_change = False
                
            try:
                _ = game_item.updated
            except AttributeError:
                game_item.updated = None
                
            try:
                _ = game_item.old_updated
            except AttributeError:
                game_item.old_updated = None
                
            skipfile_skip = check_skip_file(game_item.name, skipfiles)
            if skipfile_skip:
                info('     skip       %s (matches "%s")' % (game_item.name, skipfile_skip))
                continue

            dest_file = os.path.join(item_homedir, game_item.name)
            downloading_file = os.path.join(item_downloaddir, game_item.name)
            provisional_file = os.path.join(item_provisionaldir,game_item.name)

            if game_item.size is None:
                warn('     unknown    %s has no size info.  skipping' % game_item.name)
                continue

            if os.path.isfile(provisional_file):
                if os.path.isfile(dest_file):
                    #I don't know how you got it here, but if you did , clean up your mess! This is not my problem. But more politely. 
                    warn('     error      %s has both provisional and destination file. Please remove one.' % game_item.name)
                    continue
                else:
                    info('     working    %s' % game_item.name)
                    provisional_dict[dest_file] = (dest_file,provisional_file,game_item,all_items)
                    continue
                    
                
            if os.path.isfile(dest_file):
                if game_item.size != os.path.getsize(dest_file):
                    warn('     fail       %s has incorrect size.' % game_item.name)
                elif game_item.force_change == True:
                    warn('     fail       %s has been marked for change.' % game_item.name)
                else:
                    info('     pass       %s' % game_item.name)
                    continue  # move on to next game item
            
            if downloadLimit is not None and ((sum(sizes.values()) + game_item.size) > downloadLimit):
                info('     skip       %s (size %s would exceed download limit (%s/%s) )' % (game_item.name, megs(game_item.size),megs(sum(sizes.values())),megs(downloadLimit)))
                continue

            
            info('     download   %s' % game_item.name)
            sizes[dest_file] = game_item.size
            
        
            work_dict[dest_file] = (game_item.href, game_item.size, 0, game_item.size-1, dest_file,downloading_file,provisional_file,game_item,all_items)
    
    for work_item in work_dict:
        work.put(work_dict[work_item])
    
    for provisional_item in provisional_dict:
        work_provisional.put(provisional_dict[provisional_item])

    if dryrun:
        info("{} left to download".format(gigs(sum(sizes.values()))))
        return  # bail, as below just kicks off the actual downloading
        
    if work.empty():
        info("nothing to download")
        return
    
    downloading_root_dir = os.path.join(savedir, DOWNLOADING_DIR_NAME)
    if not os.path.isdir(downloading_root_dir):
        os.makedirs(downloading_root_dir)

    provisional_root_dir = os.path.join(savedir, DOWNLOADING_DIR_NAME,PROVISIONAL_DIR_NAME)
    if not os.path.isdir(provisional_root_dir):
        os.makedirs(provisional_root_dir)        

    info('-'*60)

    # downloader worker thread main loop
    def worker():
        tid = threading.current_thread().ident
        while not work.empty():
            (href, sz, start, end, path,downloading_path,provisional_path,writable_game_item,work_writable_items) = work.get()
            try:
                # Proactively refresh token before each download to prevent expiration during transfer
                check_and_renew_token(downloadSession, proactive_buffer=300)  # Refresh if < 5 min left
                
                dest_dir = os.path.dirname(path)
                downloading_dir = os.path.dirname(downloading_path)
                provisional_dir = os.path.dirname(provisional_path)
                compat_downloading_path = process_path(downloading_path)
                with lock:
                    if not os.path.isdir(dest_dir):
                        os.makedirs(dest_dir)
                    if not os.path.isdir(downloading_dir):    
                        os.makedirs(downloading_dir)                    
                    if not os.path.isdir(provisional_dir):    
                        os.makedirs(provisional_dir) 
                    if (os.path.exists(path)):    
                        info("moving existing file '%s' to '%s' for downloading " % (path,downloading_path))
                        shutil.move(path,downloading_path)
                        file_sz = os.path.getsize(downloading_path)    
                        if file_sz > sz:  # if needed, truncate file if ours is larger than expected size
                            with open_notrunc(downloading_path) as f:
                                f.truncate(sz)
                        if file_sz < sz: #preallocate extra space
                            preallocate_file(downloading_path, sz, skippreallocation)
                    else:
                        if (os.path.exists(downloading_path)):
                            file_sz = os.path.getsize(downloading_path)    
                            if file_sz > sz:  # if needed, truncate file if ours is larger than expected size
                                with open_notrunc(downloading_path) as f:
                                    f.truncate(sz)
                            if file_sz < sz: #preallocate extra space       
                                preallocate_file(downloading_path, sz, skippreallocation)
                        else:
                            preallocate_file(downloading_path, sz, skippreallocation)
                succeed = False
                actual_sz = None
                response = request_head(downloadSession,href)
                
                # Check for size mismatch early and adjust before downloading
                if 'content-length' in response.headers:
                    reported_sz = int(response.headers['content-length'])
                    if reported_sz != sz:
                        with lock:
                            warn("manifest size mismatch for %s: manifest=%d, server=%d - adjusting" 
                                 % (os.path.basename(path), sz, reported_sz))
                        size_diff = reported_sz - sz
                        with lock:
                            sizes[path] += size_diff
                        # Update sz and end for the download
                        sz = reported_sz
                        end = sz - 1
                        actual_sz = reported_sz
                        # Resize the file if needed
                        if os.path.exists(downloading_path):
                            with open_notrunc(downloading_path) as f:
                                f.truncate(sz)
                        else:
                            preallocate_file(downloading_path, sz, skippreallocation)
                
                chunk_tree = fetch_chunk_tree(response,downloadSession)
                if (chunk_tree is not None):
                    # Download using chunk verification
                    succeed, detected_sz = download_with_chunk_verification(downloading_path, href, sz, path, sizes, lock, downloadSession, tid, chunk_tree, rates)
                    if detected_sz is not None and actual_sz is None:
                        actual_sz = detected_sz
                else:
                    # Download without chunk verification
                    succeed, detected_sz = download_without_chunks(downloading_path, href, start, end, sz, path, sizes, lock, downloadSession, tid, rates)
                    if detected_sz is not None and actual_sz is None:
                        actual_sz = detected_sz
                
                if succeed and sizes[path]==0:
                    with lock:
                        info("moving provisionally completed download '%s' to '%s'  " % (downloading_path,provisional_path))
                        shutil.move(downloading_path,provisional_path)
                        #if writable_game_item != None:
                        #    try:
                        #        _ = writable_game_item.force_change
                        #    except AttributeError:
                        #        writable_game_item.force_change = False
                        #    try:
                        #        _ = writable_game_item.updated
                        #    except AttributeError:
                        #        writable_game_item.updated = None
                        #    try:
                        #        _ = writable_game_item.old_updated
                        #    except AttributeError:
                        #        writable_game_item.old_updated = None
                        #    try:
                        #        _ = writable_game_item.prev_verified
                        #    except AttributeError:
                        #        writable_game_item.prev_verified = False

                        #    wChanged = False;
                        #    if writable_game_item.force_change:
                        #        writable_game_item.force_change = False
                        #        writable_game_item.old_updated = writable_game_item.updated
                        #        wChanged = True
                        #    if writable_game_item.prev_verified:
                        #        writable_game_item.prev_verified = False
                        #        wChanged = True
                        #    if wChanged:  
                        #        save_manifest(work_writable_items)
                    #This should be thread safe so should be fine outside the lock, doing it after the lock so we don't add this if something went wrong.
                    work_provisional.put((path,provisional_path,writable_game_item,work_writable_items)) 
                else:
                    with lock:
                        info("not moving uncompleted download '%s', success: %s remaining bytes: %d / %d " % (downloading_path,str(succeed),sizes[path],sz))
            except IOError as e:
                with lock:
                    warn("The handled exception was:")
                    log_exception('')
                    warn("End exception report.")
                    print('!', path, file=sys.stderr)
                    errors.setdefault(path, []).append(e)
            except Exception as e:
                 with lock:
                    warn("The unhandled exception was:")
                    log_exception('')
                    warn("End exception report.")
                    raise
            #debug 
            #info("thread completed")
            work.task_done()

    # detailed progress report
    last_line_count = [0]  # Track how many lines we printed last time
    
    def progress():
        with lock:
            left = sum(sizes.values())
            progress_lines = []
            
            for path, flowrates in sorted(rates.items()):
                flows = {}
                for tid, (sz, t) in flowrates:
                    szs, ts = flows.get(tid, (0, 0))
                    flows[tid] = sz + szs, t + ts
                bps = sum(szs/ts for szs, ts in list(flows.values()) if ts > 0)
                progress_line = '%10s %8.1fMB/s %2dx  %s' % \
                    (megs(sizes[path]), bps / 1024.0**2, len(flows), "%s/%s" % (os.path.basename(os.path.split(path)[0]), os.path.split(path)[1]))
                progress_lines.append(progress_line)
            
            if len(rates) != 0:  # only update if there's change
                remaining_text = '%s remaining' % gigs(left)
                progress_lines.append(remaining_text)
                display_progress_lines(progress_lines, last_line_count)
            
            rates.clear()

    # process work items with a thread pool
    lock = threading.Lock()
    pool = []
    for i in range(HTTP_GAME_DOWNLOADER_THREADS):
        t = threading.Thread(target=worker)
        t.daemon = True
        t.start()
        pool.append(t)
    try:
        while any(t.is_alive() for t in pool):
            progress()
            time.sleep(1)
    except KeyboardInterrupt:
        # Move cursor down past progress lines and print newline
        if last_line_count[0] > 0:
            sys.stdout.write('\033[%dB' % last_line_count[0])
        sys.stdout.write('\n')
        sys.stdout.flush()
        raise
    except Exception:
        # Move cursor down past progress lines and print newline
        if last_line_count[0] > 0:
            sys.stdout.write('\033[%dB' % last_line_count[0])
        sys.stdout.write('\n')
        sys.stdout.flush()
        with lock:
            warn("The unhandled exception was:")
            log_exception('')
            warn("End exception report.")
        raise
    
    # Move cursor down past progress lines and print newline
    if last_line_count[0] > 0:
        sys.stdout.write('\033[%dB' % last_line_count[0])
    sys.stdout.write('\n')
    sys.stdout.flush()

    wChanged = False;
    
    #Everything here would be done inside a lock so may as well process it in the main thread.
    while not work_provisional.empty():
        (path,provisional_path,writable_game_item,work_writable_items) = work_provisional.get()
        info("moving provisionally completed download '%s' to '%s'  " % (provisional_path,path))
        shutil.move(provisional_path,path)
        if writable_game_item != None:
            try:
                _ = writable_game_item.force_change
            except AttributeError:
                writable_game_item.force_change = False
            try:
                _ = writable_game_item.updated
            except AttributeError:
                writable_game_item.updated = None
            try:
                _ = writable_game_item.old_updated
            except AttributeError:
                writable_game_item.old_updated = None
            try:
                _ = writable_game_item.prev_verified
            except AttributeError:
                writable_game_item.prev_verified = False

            
            if writable_game_item.force_change:
                writable_game_item.force_change = False
                writable_game_item.old_updated = writable_game_item.updated
                wChanged = True
            if writable_game_item.prev_verified:
                writable_game_item.prev_verified = False
                wChanged = True
    if wChanged:  
        save_manifest(work_writable_items)

    
    # Force garbage collection to release any file handles
    import gc
    gc.collect()
    time.sleep(1)  # Give Windows time to process file handle releases
    
    # Cleanup empty directories with aggressive retry strategy
    def safe_remove_empty_dir(path, dir_name, dir_type):
        """Aggressively remove empty directory with multiple strategies."""
        import platform
        import subprocess
        
        # Convert to absolute path
        abs_path = os.path.abspath(path)
        
        try:
            if not os.listdir(abs_path):
                # Strategy 1: Normal os.rmdir
                for attempt in range(3):
                    try:
                        os.rmdir(abs_path)
                        # Verify it's actually gone
                        if not os.path.exists(abs_path):
                            info(f"Cleaned up empty {dir_type} directory: {dir_name}")
                            return True
                        time.sleep(0.5)
                    except (PermissionError, OSError):
                        if attempt < 2:
                            time.sleep(1)
                        continue
                
                # Strategy 2: shutil.rmtree with onerror handler
                def handle_remove_readonly(func, path_arg, exc):
                    """Error handler to remove read-only files."""
                    import stat
                    try:
                        if not os.access(path_arg, os.W_OK):
                            os.chmod(path_arg, stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
                            func(path_arg)
                    except Exception:
                        pass
                
                try:
                    shutil.rmtree(abs_path, onerror=handle_remove_readonly)
                    time.sleep(0.5)
                    # Verify it's actually gone
                    if not os.path.exists(abs_path):
                        info(f"Cleaned up empty {dir_type} directory (forced): {dir_name}")
                        return True
                except Exception:
                    pass
                
                # Strategy 3: Windows native rmdir command
                if platform.system() == "Windows":
                    try:
                        result = subprocess.run(['cmd', '/c', 'rmdir', '/s', '/q', abs_path], 
                                     capture_output=True, timeout=5)
                        time.sleep(0.5)
                        if not os.path.exists(abs_path):
                            info(f"Cleaned up empty {dir_type} directory (native): {dir_name}")
                            return True
                    except Exception:
                        pass
                
                # If we get here, all strategies failed
                error(f"Could not remove empty {dir_type} directory: {dir_name} at {abs_path}")
                error(f"  Directory still exists: {os.path.exists(abs_path)}")
                error(f"  Directory is empty: {not os.listdir(abs_path) if os.path.exists(abs_path) else 'N/A'}")
        except Exception as e:
            error(f"Exception during cleanup of {dir_name}: {e}")
        return False
    
    for dir in os.listdir(downloading_root_dir):
        if dir != PROVISIONAL_DIR_NAME:
            testdir = os.path.join(downloading_root_dir, dir)
            if os.path.isdir(testdir):
                safe_remove_empty_dir(testdir, dir, "downloading")

    for dir in os.listdir(provisional_root_dir):
        testdir = os.path.join(provisional_root_dir, dir)
        if os.path.isdir(testdir):
            safe_remove_empty_dir(testdir, dir, "provisional")
                    
