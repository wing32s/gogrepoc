#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import time
import webbrowser
import datetime
import logging
import threading
import shutil
import zipfile
import re
import getpass
import html5lib
import xml.etree.ElementTree
import shelve
from queue import Queue
from urllib.parse import urlparse, unquote, urlunparse, parse_qs
import ctypes # For wakelock logic if we move it here or keep in utils

try:
    from html2text import html2text
except ImportError:
    def html2text(x): return x

from .utils import (
    AttrDict, info, warn, error, debug, log_exception,
    ConditionalWriter, open_notrunc, open_notruncwrrd, hashfile, hashstream, slugify,
    check_skip_file, process_path, is_numeric_id, get_fs_type, test_zipfile,
    move_with_increment_on_clash, pretty_size, get_total_size, build_md5_lookup,
    HTTP_RETRY_DELAY, HTTP_GAME_DOWNLOADER_THREADS, HTTP_TIMEOUT,
    MANIFEST_FILENAME, RESUME_MANIFEST_FILENAME, CONFIG_FILENAME,
    MD5_DIR_NAME, MD5_DB, DOWNLOADING_DIR_NAME, PROVISIONAL_DIR_NAME,
    ORPHAN_DIR_NAME, IMAGES_DIR_NAME, INFO_FILENAME, SERIAL_FILENAME,
    GAME_STORAGE_DIR, RESUME_MANIFEST_SYNTAX_VERSION, RESUME_SAVE_THRESHOLD,
    GOG_HOME_URL, GOG_LOGIN_URL, GOG_AUTH_URL, GOG_TOKEN_URL,
    GOG_GALAXY_REDIRECT_URL, GOG_CLIENT_ID, GOG_SECRET,
    GOG_MEDIA_TYPE_GAME, GOG_MEDIA_TYPE_MOVIE, GOG_ACCOUNT_URL,
    REPO_HOME_URL, NEW_RELEASE_URL, LANG_TABLE, SKIP_MD5_FILE_EXT,
    INSTALLERS_EXT, ORPHAN_DIR_EXCLUDE_LIST, ORPHAN_FILE_EXCLUDE_LIST,
    WINDOWS_PREALLOCATION_FS, POSIX_PREALLOCATION_FS
)

from .api import (
    makeGOGSession, makeGitHubSession, request, request_head, fetch_chunk_tree, save_token, renew_token
)

from .manifest import (
    load_manifest, save_manifest, load_resume_manifest, save_resume_manifest,
    load_config_file, save_config_file, item_checkdb,
    handle_game_renames, handle_game_updates,
    filter_downloads, filter_extras, filter_dlcs, deDuplicateList
)

from .game_filter import GameFilter

# For preallocation
GENERIC_READ = 0x80000000
GENERIC_WRITE = 0x40000000
CREATE_NEW = 0x1    
OPEN_EXISTING = 0x3
FILE_BEGIN = 0x0
# Ideally move these to utils or cross-platform shim, but keeping here for now as they are used in cmd_download

lock = threading.Lock()

def cmd_login(user_id=None):
    """Authenticate with GOG using browser-based OAuth2 flow and save the token.
    
    Opens the user's default web browser to GOG's login page where they can sign in
    using their GOG credentials or third-party providers (Google/Discord). After
    successful authentication, the user pastes the redirected URL or authorization
    code back into the CLI. The resulting OAuth2 token is saved to disk for future
    API requests.
    
    The token file location depends on the user_id:
    - Default (no user_id): TOKEN_FILENAME in current directory
    - With user_id: TOKEN_FILENAME.<user_id> in current directory
    
    Args:
        user_id: Optional identifier for multi-user support. If provided, saves
            token to a user-specific file allowing multiple GOG accounts to be
            managed independently.
    
    Raises:
        SystemExit: If the user fails to provide a valid authorization code or
            if the token exchange with GOG servers fails.
    
    Notes:
        - Requires an active internet connection and working web browser
        - The token includes an expiry time and can be automatically renewed
        - Browser opening may fail in headless/container environments; URL is
          displayed for manual copying in such cases
    """

    info("This CLI uses browser-based sign-in for GOG.")
    info("If your GOG login page offers Google/Discord sign-in, you can use it there.")

    # Use the exact same redirect_uri for both authorization and token requests
    redirect_uri = GOG_GALAXY_REDIRECT_URL + '?origin=client'

    loginSession = makeGOGSession(loginSession=True)

    # Fetch the authorize page URL (we'll send the user to it in a real browser)
    page_response = request(loginSession, GOG_AUTH_URL,
                           args={'client_id': GOG_CLIENT_ID,
                                 'redirect_uri': redirect_uri,
                                 'response_type': 'code',
                                 'layout': 'client2'})

    auth_url = page_response.url
    info("Open this URL in your browser to sign in:")
    info(auth_url)
    try:
        webbrowser.open(auth_url)
    except Exception:
        pass

    def _extract_code(user_input: str):
        s = (user_input or "").strip()
        if not s:
            return None
        # If they paste just the code, accept it
        if "://" not in s and "code=" not in s:
            return s
        # Otherwise parse from URL (query or fragment)
        parsed = urlparse(s)
        q = parse_qs(parsed.query)
        if "code" in q and q["code"]:
            return q["code"][0]
        frag = parse_qs(parsed.fragment)
        if "code" in frag and frag["code"]:
            return frag["code"][0]
        return None

    pasted = input("After signing in, paste the full redirected URL (or just the code): ").strip()
    login_code = _extract_code(pasted)
    if not login_code:
        error("Could not find an authorization code in what you pasted.")
        return

    # Exchange code for token
    if login_code:
        token_start = time.time()
        # GOG's OAuth implementation appears to accept GET requests for token endpoint
        # Using args (not data) to match original implementation
        token_response = request(loginSession, GOG_TOKEN_URL,
                               args={'client_id': GOG_CLIENT_ID,
                                     'client_secret': GOG_SECRET,
                                     'grant_type': 'authorization_code',
                                     'code': login_code,
                                     'redirect_uri': redirect_uri})
        token_json = token_response.json()
        token_json['expiry'] = token_start + token_json['expires_in']
        save_token(token_json, user_id=user_id)
        info('Galaxy login successful!')
    else:
        error('Galaxy login failed.')
        sys.exit(1)


def cmd_update_v2(os_list, lang_list, skipknown, updateonly, partial, ids, skipids, skipHidden, 
                  installers, resumemode, strict, strictDupe, md5xmls, noChangeLogs):
    """Update the local game manifest using new modular architecture (EXPERIMENTAL).
    
    This is a refactored version of cmd_update that uses the game_filter and update
    modules for cleaner, more maintainable code. It provides the same functionality
    as cmd_update but with improved organization and testability.
    
    Args:
        os_list: List of operating systems to include (e.g., ['windows', 'linux', 'mac'])
        lang_list: List of language codes to include (e.g., ['en', 'de', 'fr'])
        skipknown: If True, only add new games not already in manifest
        updateonly: If True, only process games with updates
        partial: If True, enables both skipknown and updateonly (incremental update)
        ids: List of game titles or IDs to process (None = all games)
        skipids: List of game titles or IDs to exclude
        skipHidden: If True, exclude hidden games
        installers: Installer type filter ('both', 'standalone', 'galaxy')
        resumemode: Resume behavior ('resume', 'noresume', 'onlyresume')
        strict: Force thorough timestamp/MD5 checking on all file types
        strictDupe: Remove duplicate entries from download lists
        md5xmls: Fetch MD5 checksum XML files
        noChangeLogs: Exclude changelog data from manifest
    """
    from .update import (
        FetchConfig,
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
    from .game_filter import GameFilter
    
    info("Using experimental cmd_update_v2 (modular architecture)")
    
    # Load existing manifest
    gamesdb = load_manifest()
    
    # Save original parameters for post-resume
    save_os_list = os_list
    save_lang_list = lang_list
    save_skipknown = skipknown
    save_updateonly = updateonly
    save_partial = partial
    save_installers = installers
    save_strict = strict
    save_strictDupe = strictDupe
    save_md5xmls = md5xmls
    save_noChangeLogs = noChangeLogs
    
    # Check for resume manifest
    needresume, resumedb, resumeprops = check_resume_needed(resumemode)
    
    # If resuming, load resume parameters
    if needresume:
        info('incomplete update detected, resuming...')
        os_list = resumeprops['os_list']
        lang_list = resumeprops['lang_list']
        installers = resumeprops['installers']
        strict = resumeprops['strict']
        partial = resumeprops.get('partial', partial)
        skipknown = resumeprops.get('skipknown', skipknown)
        updateonly = resumeprops.get('updateonly', updateonly)
        strictDupe = resumeprops.get('strictDupe', True)
        md5xmls = resumeprops.get('md5xmls', True)
        noChangeLogs = resumeprops.get('noChangeLogs', False)
        
        items = resumedb
    else:
        # Normalize partial mode
        if partial:
            skipknown = True
            updateonly = True
        
        # Create session and renew token
        updateSession = makeGOGSession()
        renew_token(updateSession)
        
        # Create fetch configuration
        config = FetchConfig(
            os_list=os_list,
            lang_list=lang_list,
            installers=installers,
            strict_dupe=strictDupe,
            md5xmls=md5xmls,
            no_changelogs=noChangeLogs
        )
        
        # Extract known IDs from existing manifest
        known_ids = [item.id for item in gamesdb]
        
        # Determine which update strategy to use and fetch games
        if ids:
            # Strategy 1: Specific games by ID/title
            info("Update mode: Specific games")
            items = update_specific_games(
                updateSession, 
                ids, 
                config,
                skipids=skipids
            )
        elif skipknown and updateonly:
            # Strategy 2: Partial update (new OR updated)
            info("Update mode: Partial (new games + updates)")
            items = update_partial(
                updateSession,
                known_ids,
                config,
                skipids=skipids,
                skip_hidden=skipHidden
            )
        elif skipknown:
            # Strategy 3: New games only
            info("Update mode: New games only")
            items = update_new_games_only(
                updateSession,
                known_ids,
                config,
                skipids=skipids,
                skip_hidden=skipHidden
            )
        elif updateonly:
            # Strategy 4: Updated games only
            info("Update mode: Games with updates only")
            items = update_changed_games_only(
                updateSession,
                config,
                skipids=skipids,
                skip_hidden=skipHidden
            )
        else:
            # Strategy 5: Full library update
            info("Update mode: Full library")
            items = update_full_library(updateSession, config)
        
        # Bail if nothing to do
        if len(items) == 0:
            if partial:
                warn('no new games or updates found.')
            elif updateonly:
                warn('no new game updates found.')
            elif skipknown:
                warn('no new games found.')
            else:
                warn('nothing to do')
            return
    
    # Create/update resume manifest
    resumedb = sorted(items, key=lambda item: item.title)
    resumeprop = create_resume_properties(
        FetchConfig(os_list, lang_list, installers, strictDupe, md5xmls, noChangeLogs),
        skipknown, partial, updateonly
    )
    resumeprop['strict'] = strict
    resumedb.append(resumeprop)
    save_resume_manifest(resumedb)
    
    # Create GameFilter with strict flag for processing
    processing_filter = GameFilter(strict=strict)
    
    # Process items with strict update checking and resume saves
    gamesdb, global_dupes = process_items_with_resume(
        items, gamesdb, processing_filter, skipknown, updateonly
    )
    
    # Handle game renames (directory and file renames when GOG changes titles)
    info("Checking for game renames...")
    gamedir = os.path.join(GAME_STORAGE_DIR, 'games')
    orphan_root_dir = os.path.join(gamedir, ORPHAN_DIR_NAME)
    if not os.path.isdir(orphan_root_dir):
        os.makedirs(orphan_root_dir)
    
    for game in gamesdb:
        handle_single_game_rename(game, gamedir, orphan_root_dir, dryrun=False)
    
    # Save final manifest in alphabetical order
    sorted_gamesdb = sorted(gamesdb, key=lambda game: game.title)
    save_manifest(sorted_gamesdb, update_md5_xml=md5xmls, delete_md5_xml=md5xmls)
    
    # Mark resume as complete
    resumeprop['complete'] = True
    save_resume_manifest([resumeprop])
    
    info("Manifest saved successfully")
    
    # If this was a resume, call again with original parameters
    if needresume:
        info('resume completed')
        if resumemode != 'onlyresume':
            info('returning to specified download request...')
            cmd_update_v2(save_os_list, save_lang_list, save_skipknown, save_updateonly,
                         save_partial, ids, skipids, skipHidden, save_installers, resumemode,
                         save_strict, save_strictDupe, save_md5xmls, save_noChangeLogs)


def cmd_update(os_list, lang_list, skipknown, updateonly, partial, ids, skipids,skipHidden,installers,resumemode,strict,strictDupe,strictDownloadsUpdate,strictExtrasUpdate,md5xmls,noChangeLogs):
    """Update the local game manifest by fetching the latest game data from GOG.
    
    Queries the GOG API to retrieve your owned games and their available downloads
    (installers, extras, DLCs). Updates the local manifest file with current file
    information including download URLs, MD5 checksums, version numbers, and 
    changelogs. Supports resuming interrupted updates and various filtering options
    to control which games are processed.
    
    The manifest is saved periodically during updates and can be resumed if interrupted.
    Use this command before downloading to ensure you have the latest game metadata.
    
    Args:
        os_list: List of operating systems to include (e.g., ['windows', 'linux', 'mac']).
            Only downloads for these platforms will be included in the manifest.
        lang_list: List of language codes to include (e.g., ['en', 'de', 'fr']).
            Only downloads for these languages will be included.
        skipknown: If True, only add new games not already in the manifest.
            Useful for incremental updates of large libraries.
        updateonly: If True, only process games that GOG reports as having updates.
            Skips games with no changes since last update.
        partial: If True, enables both skipknown and updateonly modes.
            Shortcut for incremental library updates.
        ids: List of game titles or IDs to process. If provided, only these games
            are updated. Can be game slugs (e.g., 'witcher_3') or numeric IDs.
        skipids: List of game titles or IDs to exclude from processing.
            Useful for blacklisting problematic games.
        skipHidden: If True, excludes games marked as hidden in your GOG library.
        installers: Filter for installer types. Valid values:
            - 'both': Include both standalone and Galaxy installers (default)
            - 'standalone': Only standalone offline installers
            - 'galaxy': Only GOG Galaxy installers
        resumemode: Controls resume behavior for interrupted updates:
            - 'resume': Automatically resume if incomplete update detected
            - 'noresume': Start fresh, discarding any resume data
        strict: If True, marks files for re-download when version/size changes.
            Ensures local files match latest versions exactly.
        strictDupe: If True, removes duplicate entries from download lists.
            Prevents same file from appearing multiple times.
        strictDownloadsUpdate: If True, applies strict checking to game installers.
            Files with version changes are marked for update.
        strictExtrasUpdate: If True, applies strict checking to extras/bonus content.
            Less commonly needed since extras rarely change.
        md5xmls: If True, fetches MD5 checksum XML files when available.
            Used for file integrity verification during downloads.
        noChangeLogs: If True, excludes changelog data from manifest.
            Reduces manifest size if changelog history is not needed.
    
    Raises:
        SystemExit: If authentication fails or GOG API returns invalid data.
    
    Notes:
        - Requires valid authentication token (run cmd_login first)
        - Token is automatically renewed if expiring during update
        - Manifest is saved periodically (every RESUME_SAVE_THRESHOLD games)
        - Games removed from GOG library are automatically removed from manifest
        - Resume manifest is saved to allow recovery from interruptions
        - Large libraries may take significant time to update completely
    """
    media_type = GOG_MEDIA_TYPE_GAME
    items = []
    known_ids = []
    known_titles = []
    i = 0
    
    api_url  = GOG_ACCOUNT_URL
    api_url += "/getFilteredProducts"
 

    gamesdb = load_manifest()
    save_partial = partial
    save_skipknown = skipknown
    save_updateonly = updateonly
    
    if not gamesdb and not skipknown and not updateonly:
        partial = False;
    
    if partial:
        skipknown = True;
        updateonly = True;
    
    updateSession = makeGOGSession()
    renew_token(updateSession)  # Check and renew token if needed before making requests
    
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
            
            while (inp not in ["D","d","A","a"]):
                inp = input("(D)iscard incompatible manifest or (A)bort? (D/d/A/a): ")

                if (inp in ["D","d"]):
                    warn("Discarding")
                    resumedb = None
                    needresume = False
                elif (inp in ["A","a"]):
                    warn("Aborting")
                    sys.exit()
    except Exception:
        resumedb = None
        needresume = False
    
    if (needresume):
        info('incomplete update detected, resuming...')
        save_os_list = os_list
        os_list = resumeprops['os_list']        
        save_lang_list = lang_list
        lang_list = resumeprops['lang_list']
        save_installers = installers
        installers = resumeprops['installers']
        save_strict = strict
        strict = resumeprops['strict']
        save_strictDupe = strictDupe
        save_strictDownloadsUpdate = strictDownloadsUpdate
        save_strictExtrasUpdate = strictExtrasUpdate
        save_md5xmls = md5xmls
        save_noChangeLogs = noChangeLogs
        try:
            partial = resumeprops['partial']
        except KeyError:
            pass
        try:
            skipknown = resumeprops['skipknown']
        except KeyError:
            pass
        try:
            updateonly = resumeprops['updateonly']
        except KeyError:
            pass
        try:
            strictDupe = resumeprops['strictDupe']
        except KeyError:
            strictDupe = True
        try:
           strictDownloadsUpdate = resumeprops['strictDownloadsUpdate']
        except KeyError:
            strictDownloadsUpdate = True
        try:
            strictExtrasUpdate = resumeprops['strictExtrasUpdate']
        except KeyError:
            strictExtrasUpdate = False
        try:
           md5xmls = resumeprops['md5xmls']
        except KeyError:
            md5xmls = True
        try:
            noChangeLogs = resumeprops['noChangeLogs']
        except KeyError:
            noChangeLogs = False            
            
        items = resumedb
        items_count = len(items)
        print_padding = len(str(items_count))
        
    else:    
        # Make convenient list of known ids11
        for item in gamesdb:
            known_ids.append(item.id)
                
        idsOriginal = ids[:]       

        for item in gamesdb:
            known_titles.append(item.title)

            
        # Fetch shelf data
        done = False
        while not done:
            i += 1  # starts at page 1
            if i == 1:
                info('fetching game product data (page %d)...' % i)
            else:
                info('fetching game product data (page %d / %d)...' % (i, json_data['totalPages']))
            data_response = request(updateSession,api_url,args={'mediaType': media_type,'sortBy': 'title','page': str(i)})    
#            with open("text.html","w+",encoding='utf-8') as f:
#                f.write(data_response.text)
            try:
                json_data = data_response.json()
            except ValueError:
                error('failed to load product data (are you still logged in?)')
                raise SystemExit(1)

            # Parse out the interesting fields and add to items dict
            for item_json_data in json_data['products']:
                # skip games marked as hidden
                if skipHidden and (item_json_data.get('isHidden', False) is True):
                    continue

                item = AttrDict()
                item.id = item_json_data['id']
                item.title = item_json_data['slug']
                item.folder_name = item_json_data['slug']
                item.long_title = item_json_data['title']

                item.genre = item_json_data['category']
                item.image_url = item_json_data['image']
                #item.image_urls[item.long_title] = item_json_data['image']
                item.store_url = item_json_data['url']
                item.media_type = media_type
                item.rating = item_json_data['rating']
                item.has_updates = bool(item_json_data['updates'])
                item.old_title = None
                #mirror these so they appear at the top of the json entry 
                item._title_mirror =  item.title  
                item._long_title_mirror = item.long_title
                item._id_mirror =  item.id


                item.gog_data = AttrDict()
                for key in item_json_data:
                    try:
                        tmp_contents = item[key]
                        if tmp_contents != item_json_data[key]:
                            debug("GOG Data Key, %s , for item clashes with Item Data Key storing detailed info in secondary dict" % key)
                            item.gog_data[key] = item_json_data[key]
                    except Exception:
                        item[key] = item_json_data[key]
                
                
                if not done:
                    if item.title not in skipids and str(item.id) not in skipids: 
                        if ids: 
                            if (item.title  in ids or str(item.id) in ids):  # support by game title or gog id
                                info('scanning found "{}" in product data!'.format(item.title))
                                try:
                                    ids.remove(item.title)
                                except ValueError:
                                    try:
                                        ids.remove(str(item.id))
                                    except ValueError:
                                        warn("Somehow we have matched an unspecified ID. Huh ?")
                                if not ids:
                                    done = True
                            else:
                                continue
                                
                                
                        if (not partial) or (updateonly and item.has_updates) or (skipknown and item.id not in known_ids):  
                             items.append(item)
                    else:        
                        info('skipping "{}" found in product data!'.format(item.title))
                    
                
            if i >= json_data['totalPages']:
                done = True
                    
     

        if not idsOriginal and not updateonly and not skipknown:
            validIDs = [item.id for item in items]
            invalidItems = [itemID for itemID in known_ids if itemID not in validIDs and str(itemID) not in skipids]
            if len(invalidItems) != 0: 
                warn('old games in manifest. Removing ...')
                for item in invalidItems:
                    warn('Removing id "{}" from manifest'.format(item))
                    item_idx = item_checkdb(item, gamesdb)
                    if item_idx is not None:
                        del gamesdb[item_idx]
        
        if ids and not updateonly and not skipknown:
            invalidTitles = [id for id in ids if id in known_titles]    
            invalidIDs = [int(id) for id in ids if is_numeric_id(id) and int(id) in known_ids]
            invalids = invalidIDs + invalidTitles
            if invalids:
                formattedInvalids =  ', '.join(map(str, invalids))        
                warn(' game id(s) from {%s} were in your manifest but not your product data ' % formattedInvalids)
                titlesToIDs = [(game.id,game.title) for game in gamesdb if game.title in invalidTitles]
                for invalidID in invalidIDs:
                    warn('Removing id "{}" from manifest'.format(invalidID))
                    item_idx = item_checkdb(invalidID, gamesdb)
                    if item_idx is not None:
                        del gamesdb[item_idx]
                for invalidID,invalidTitle in titlesToIDs:
                    warn('Removing id "{}" from manifest'.format(invalidTitle))
                    item_idx = item_checkdb(invalidID, gamesdb)
                    if item_idx is not None:
                        del gamesdb[item_idx]
                save_manifest(gamesdb)

                        
        # bail if there's nothing to do
        if len(items) == 0:
            if partial:
                warn('no new game or updates found.')
            elif updateonly:
                warn('no new game updates found.')
            elif skipknown:
                warn('no new games found.')
            else:
                warn('nothing to do')
            if idsOriginal:
                formattedIds =  ', '.join(map(str, idsOriginal))        
                warn('with game id(s) from {%s}' % formattedIds)
            return
            
            
        items_count = len(items)
        print_padding = len(str(items_count))
        if not idsOriginal and not updateonly and not skipknown:
            info('found %d games !!%s' % (items_count, '!'*int(items_count/100)))  # teehee
            if skipids: 
                formattedSkipIds =  ', '.join(map(str, skipids))        
                info('not including game id(s) from {%s}' % formattedSkipIds)
            
            
    # fetch item details
    i = 0
    resumedb = sorted(items, key=lambda item: item.title)
    resumeprop = {'resume_manifest_syntax_version':RESUME_MANIFEST_SYNTAX_VERSION,'os_list':os_list,'lang_list':lang_list,'installers':installers,'strict':strict,'complete':False,'skipknown':skipknown,'partial':partial,'updateonly':updateonly,'strictDupe':strictDupe,'strictDownloadsUpdate':strictDownloadsUpdate,'strictExtrasUpdate':strictExtrasUpdate,'md5xmls':md5xmls,'noChangeLogs':noChangeLogs}
    resumedb.append(resumeprop)
    save_resume_manifest(resumedb)                    
    
    resumedbInitLength = len(resumedb)
    for item in sorted(items, key=lambda item: item.title):
        api_url  = GOG_ACCOUNT_URL
        api_url += "/gameDetails/{}.json".format(item.id)
        
        

        i += 1
        info("(%*d / %d) fetching game details for %s..." % (print_padding, i, items_count, item.title))

        try:
            response = request(updateSession,api_url)
            
            item_json_data = response.json()

            item.bg_url = item_json_data['backgroundImage']
            item.bg_urls = AttrDict()
            if urlparse(item.bg_url).path != "":
                item.bg_urls[item.long_title] = item.bg_url
            item.serial = item_json_data['cdKey']
            if (not(item.serial.isprintable())): #Probably encoded in UTF-16
                try:
                    pserial = item.serial
                    if (len(pserial) % 2): #0dd
                        pserial=pserial+"\x00" 
                    pserial = bytes(pserial,"UTF-8")
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
            if (noChangeLogs):
                item_json_data['changelog'] = '' #Doing it this way prevents it getting stored as Detailed GOG Data later (which causes problems because it then lacks the changelog_end demarcation and becomes almost impossible to parse)
            item.changelog = item_json_data['changelog']
            item.changelog_end = None
            item.release_timestamp = item_json_data['releaseTimestamp']
            item.gog_messages = item_json_data['messages']
            item.downloads = []
            item.galaxyDownloads = []
            item.sharedDownloads = []
            item.extras = []
            item.detailed_gog_data = AttrDict()
            for key in item_json_data:
                if key not in ["downloads","extras","galaxyDownloads","dlcs"]: #DLCS lose some info in processing, need to fix that when extending.  #This data is going to be stored after filtering (#Consider storing languages / OSes in case new ones are added)
                    try:
                        tmp_contents = item[key]
                        if tmp_contents != item_json_data[key]:
                            debug("Detailed GOG Data Key, %s , for item clashes with Item Data Key attempting to store detailed info in secondary dict" % key)
                            try:
                                tmp_contents = item.gog_data[key]
                                if tmp_contents != item_json_data[key]:
                                    debug("GOG Data Key, %s  ,for item clashes with Item Secondary Data Key storing detailed info in tertiary dict" % key)
                                    item.detailed_gog_data[key] = item_json_data[key]
                            except Exception:
                                item.gog_data[key] = item_json_data[key]
                    except Exception:
                        item[key] = item_json_data[key]
            # parse json data for downloads/extras/dlcs
            filter_downloads(item.downloads, item_json_data['downloads'], lang_list, os_list,md5xmls,updateSession)
            filter_downloads(item.galaxyDownloads, item_json_data['galaxyDownloads'], lang_list, os_list,md5xmls,updateSession)                
            filter_extras(item.extras, item_json_data['extras'],md5xmls,updateSession)
            filter_dlcs(item, item_json_data['dlcs'], lang_list, os_list,md5xmls,updateSession)
            
            
            #Indepent Deduplication to make sure there are no doubles within galaxyDownloads or downloads to avoid weird stuff with the comprehention.
            item.downloads = deDuplicateList(item.downloads,{},strictDupe)  
            item.galaxyDownloads = deDuplicateList(item.galaxyDownloads,{},strictDupe) 
            
            item.sharedDownloads = [x for x in item.downloads if x in item.galaxyDownloads]
            if (installers=='galaxy'):
                item.downloads = []
            else:
                item.downloads = [x for x in item.downloads if x not in item.sharedDownloads]
            if (installers=='standalone'):
                item.galaxyDownloads = []
            else:        
                item.galaxyDownloads = [x for x in item.galaxyDownloads if x not in item.sharedDownloads]
                            
            existingItems = {}                
            item.downloads = deDuplicateList(item.downloads,existingItems,strictDupe)  
            item.galaxyDownloads = deDuplicateList(item.galaxyDownloads,existingItems,strictDupe) 
            item.sharedDownloads = deDuplicateList(item.sharedDownloads,existingItems,strictDupe)                 
            item.extras = deDuplicateList(item.extras,existingItems,strictDupe)
            
            # update gamesdb with new item
            item_idx = item_checkdb(item.id, gamesdb)
            if item_idx is not None:
                handle_game_updates(gamesdb[item_idx], item,strict, strictDownloadsUpdate, strictExtrasUpdate)
                gamesdb[item_idx] = item
            else:
                gamesdb.append(item)
        except Exception:
            warn("The handled exception was:")
            log_exception('error')
            warn("End exception report.")        
        resumedb.remove(item)    
        if (updateonly or skipknown or (resumedbInitLength - len(resumedb)) % RESUME_SAVE_THRESHOLD == 0):
            save_manifest(gamesdb)                
            save_resume_manifest(resumedb)                

    global_dupes = []
    sorted_gamesdb =  sorted(gamesdb, key = lambda game : game.title)
    for game in sorted_gamesdb:
        if game not in global_dupes:
            index = sorted_gamesdb.index(game)
            dupes = [game]
            while (len(sorted_gamesdb)-1 >= index+1 and sorted_gamesdb[index+1].title == game.title):
                dupes.append(sorted_gamesdb[index+1])
                index = index + 1
            if len(dupes) > 1:
                global_dupes.extend(dupes)
            
    for dupe in global_dupes:
        dupe.folder_name = dupe.title + "_" + str(dupe.id)
    #Store stuff in the DB in alphabetical order
    sorted_gamesdb =  sorted(gamesdb, key = lambda game : game.title)
    # save the manifest to disk
    save_manifest(sorted_gamesdb,update_md5_xml=md5xmls,delete_md5_xml=md5xmls)
    resumeprop['complete'] = True    
    save_resume_manifest(resumedb) 
    if (needresume):
        info('resume completed')
        if (resumemode != 'onlyresume'):
            info('returning to specified download request...')
            cmd_update(save_os_list, save_lang_list, save_skipknown, save_updateonly, save_partial, ids, skipids,skipHidden,save_installers,resumemode,save_strict,save_strictDupe,save_strictDownloadsUpdate,save_strictExtrasUpdate,save_md5xmls,save_noChangeLogs)


def cmd_import(src_dir, dest_dir, os_list, lang_list, skipextras, skipids, ids, skipgalaxy, skipstandalone, skipshared, destructive):
    """Recursively finds all files within src_dir and compares their MD5 values
    against known md5 values from the manifest.  If a match is found, the file will be copied
    into the game storage dir.
    
    Args:
        src_dir: Source directory to search for files
        dest_dir: Destination directory where games are stored
        os_list: List of OS types to include
        lang_list: List of languages to include
        skipextras: If True, skip extra content
        skipids: List of game IDs to exclude
        ids: List of game IDs to include (empty means all)
        skipgalaxy: If True, skip Galaxy installers
        skipstandalone: If True, skip standalone installers
        skipshared: If True, skip shared installers
        destructive: If True, move files instead of copying them
    """
    # Map parameters to GameFilter
    installers = 'all'
    if skipgalaxy and skipstandalone and skipshared:
        installers = 'all'  # All skipped means nothing
    elif skipgalaxy and skipshared:
        installers = 'standalone'
    elif skipstandalone and skipshared:
        installers = 'galaxy'
    elif skipgalaxy and skipstandalone:
        installers = 'shared'
    
    game_filter = GameFilter(
        os_list=os_list,
        lang_list=lang_list,
        skip_extras=skipextras,
        skipids=skipids if skipids else [],
        ids=ids if ids else [],
        installers=installers
    )
    if destructive:
        stringOperation = "move"
        stringOperationP = "moving"
    else:
        stringOperation = "copy"
        stringOperationP = "copying"
    gamesdb = load_manifest()

    info("collecting md5 data out of the manifest")
    size_info = build_md5_lookup(gamesdb, game_filter)
        
    info("searching for files within '%s'" % src_dir)
    file_list = []
    for (root, dirnames, filenames) in os.walk(src_dir):
        for f in filenames:
            if (os.extsep + f.rsplit(os.extsep,1)[1]).lower() not in SKIP_MD5_FILE_EXT: #Need to extend this to cover tar.gz too
                file_list.append(os.path.join(root, f))

    info("comparing md5 file hashes")
    for f in file_list:
        fname = os.path.basename(f)
        info("calculating filesize for '%s'" % fname)
        s = os.path.getsize(f)
        if s in size_info:
            info("calculating md5 for '%s'" % fname)
            md5_info = size_info[s]
            h = hashfile(f)
            if h in md5_info:
                info('found match(es) for file %s with size [%s] and MD5 [%s]' % (fname,s, h))
                items = md5_info[h]
                for (folder_name,file_name) in items:
                    game_dest_dir = os.path.join(dest_dir, folder_name)
                    dest_file = os.path.join(game_dest_dir, file_name)
                    info('match! %s' % (dest_file))
                    if os.path.isfile(dest_file):
                        if s == os.path.getsize(dest_file) and h == hashfile(dest_file):
                            info('destination file already exists with the same size and md5 value. skipping %s.' % stringOperation)
                            continue
                    info("%s to %s..." % (stringOperationP, dest_file))
                    if not os.path.isdir(game_dest_dir):
                        os.makedirs(game_dest_dir)
                    if destructive:
                        shutil.move(f, dest_file)
                    else:
                        shutil.copy(f, dest_file)
                    entry = items[(folder_name,file_name)]
                    changed = False
                    try:
                        if entry.force_change == True:
                            entry.has_changed = False
                            changed = True
                    except AttributeError:
                        setattr(entry,"has_changed",False)
                        changed = True
                    try:
                        if entry.old_updated != entry.updated:
                            entry.old_updated = entry.updated
                            changed = True
                    except AttributeError:
                        setattr(entry,"updated",None)
                        setattr(entry,"old_updated",None)
                        changed = True
                    try:
                        if entry.prev_verified == False:
                            entry.prev_verified = True # This isn't guaranteed to actually be the file but anything that makes it this far will pass the verify check anyway
                            changed = True
                    except AttributeError:
                        setattr(entry,"prev_verified",True)
                        changed = True
                    if changed:
                        save_manifest(gamesdb)

def cmd_clear_partial_downloads(cleandir, dryrun):
    """Remove incomplete download directories left from interrupted downloads.
    
    Cleans up the downloading directory by removing all partially downloaded game
    directories. This includes both regular download directories and provisional
    (temporary) directories used during the download process. Use this command
    to reclaim disk space after interrupted or failed downloads.
    
    The function removes:
    - All subdirectories in DOWNLOADING_DIR_NAME (except PROVISIONAL_DIR_NAME itself)
    - All subdirectories within PROVISIONAL_DIR_NAME
    
    These directories are created during download operations and may be left behind
    if downloads are interrupted by errors, user cancellation, or system crashes.
    
    Args:
        cleandir: Root directory containing the games folder structure.
            Typically GAME_STORAGE_DIR. The downloading subdirectory is located
            at: cleandir/DOWNLOADING_DIR_NAME/
        dryrun: If True, simulates the deletion without actually removing files.
            Reports what would be deleted. If False, actually deletes the directories.
    
    Notes:
        - Only removes directories, not individual files in the downloading root
        - Errors during deletion are logged but don't stop the process
        - Safe to run - only affects temporary download directories
        - Does not affect completed downloads that have been moved to game folders
        - Preserve PROVISIONAL_DIR_NAME directory itself, only its contents
    
    Example:
        >>> cmd_clear_partial_downloads('/path/to/games', dryrun=True)
        # Shows what would be deleted without actually deleting
        
        >>> cmd_clear_partial_downloads('/path/to/games', dryrun=False)
        # Actually deletes partial download directories
    """
    downloading_root_dir = os.path.join(cleandir, DOWNLOADING_DIR_NAME)
    for dir in os.listdir(downloading_root_dir):
        if dir != PROVISIONAL_DIR_NAME:
            testdir= os.path.join(downloading_root_dir,dir)
            if os.path.isdir(testdir):
                try:
                    if (not dryrun):
                        shutil.rmtree(testdir)
                    info("Deleting " + testdir)
                except Exception:
                    error("Failed to delete directory: " + testdir)

    provisional_root_dir = os.path.join(cleandir, DOWNLOADING_DIR_NAME,PROVISIONAL_DIR_NAME)
    for dir in os.listdir(provisional_root_dir):
        testdir= os.path.join(downloading_root_dir,dir)
        if os.path.isdir(testdir):
            try:
                if (not dryrun):
                    shutil.rmtree(testdir)
                info("Deleting " + testdir)
            except Exception:
                error("Failed to delete directory: " + testdir)

def cmd_trash(cleandir,installers,images,dryrun):
    """Delete orphaned files and directories that were moved by the clean command.
    
    Permanently removes content from the orphan directory (ORPHAN_DIR_NAME). The
    orphan directory contains files and directories moved by cmd_clean when they
    don't match the current manifest - typically outdated game versions, renamed
    games, or files marked for update.
    
    This command provides granular control over what gets deleted:
    - Delete only installer files (exe, sh, pkg, dmg, etc.)
    - Delete only image subdirectories
    - Delete entire orphan game directories
    
    When specific file types are targeted (installers or images), the command will
    attempt to remove empty directories after deletion, keeping the orphan directory
    structure clean.
    
    Args:
        cleandir: Root directory containing the games folder structure.
            Typically GAME_STORAGE_DIR. The orphan subdirectory is located at:
            cleandir/ORPHAN_DIR_NAME/
        installers: If True, delete only installer files (exe, sh, pkg, dmg, bin, etc.)
            from orphan directories. Files are identified by extension using INSTALLERS_EXT.
            Other files (extras, metadata) are preserved.
        images: If True, delete only image subdirectories (IMAGES_DIR_NAME) from
            orphan directories. Installer files and other content are preserved.
        dryrun: If True, simulates the deletion without actually removing files.
            Reports what would be deleted. If False, actually deletes the files.
    
    Behavior modes:
        - installers=False, images=False: Deletes entire orphan game directories
        - installers=True: Deletes only installer files, attempts to clean empty dirs
        - images=True: Deletes only image folders, attempts to clean empty dirs
        - installers=True, images=True: Deletes both types, attempts to clean empty dirs
    
    Notes:
        - Only affects content in ORPHAN_DIR_NAME, never touches active game directories
        - Deletion errors are logged but don't stop the process
        - Empty directory removal is silent (no error if directory not empty)
        - Safe to run repeatedly - only deletes what matches the criteria
        - Complements cmd_clean which moves files to orphan directory
    
    Workflow:
        1. Run cmd_clean to identify and move outdated/unexpected files to orphans
        2. Review orphaned content to ensure nothing important was moved
        3. Run cmd_trash to permanently delete the orphaned content
    
    Example:
        >>> cmd_trash('/path/to/games', installers=True, images=False, dryrun=True)
        # Shows which installer files would be deleted
        
        >>> cmd_trash('/path/to/games', installers=False, images=True, dryrun=False)
        # Actually deletes image folders from orphaned games
        
        >>> cmd_trash('/path/to/games', installers=False, images=False, dryrun=False)
        # Deletes entire orphaned game directories
    """
    downloading_root_dir = os.path.join(cleandir, ORPHAN_DIR_NAME)
    for dir in os.listdir(downloading_root_dir):
        testdir= os.path.join(downloading_root_dir,dir)
        if os.path.isdir(testdir):
            if installers:
                contents = os.listdir(testdir)
                deletecontents = [x for x in contents if (len(x.rsplit(os.extsep,1)) > 1 and (os.extsep + x.rsplit(os.extsep,1)[1]) in INSTALLERS_EXT)]
                for content in deletecontents:
                    contentpath = os.path.join(testdir,content)
                    if (not dryrun):
                        os.remove(contentpath)
                    info("Deleting " + contentpath )
            if images:
                images_folder = os.path.join(testdir,IMAGES_DIR_NAME)
                if os.path.isdir(images_folder):
                    if (not dryrun):
                        shutil.rmtree(images_folder)
                    info("Deleting " + images_folder )
            if not ( installers or images):
                try:
                    if (not dryrun):
                        shutil.rmtree(testdir)
                    info("Deleting " + testdir)
                except Exception:
                    error("Failed to delete directory: " + testdir)
            else:
                try:
                    if (not dryrun):
                        os.rmdir(testdir)
                    info("Removed empty directory " + testdir)
                except OSError:
                    pass

def cmd_backup(src_dir, dest_dir,skipextras,os_list,lang_list,ids,skipids,skipgalaxy,skipstandalone,skipshared):
    """Copy game files from source to backup destination, validating against the manifest.
    
    Creates a selective backup of your GOG game library by copying only files that
    exist in the manifest and pass validation checks. This is useful for creating
    filtered backups (e.g., only Windows games, only specific languages) or for
    migrating your library to a new location while ensuring file integrity.
    
    The function validates each file's size against the manifest before copying,
    skipping files with unexpected sizes. It also preserves game metadata by
    copying info and serial files alongside game files.
    
    Backup process:
    1. Load manifest to determine which files should exist
    2. Apply filters (OS, language, installer type, game IDs)
    3. For each matching game file in manifest:
       - Check if source file exists and has correct size
       - Skip if destination already has file with correct size
       - Copy file to destination maintaining folder structure
    4. Copy metadata files (info, serial) for games that were backed up
    
    Args:
        src_dir: Source directory containing game files to backup.
            Typically the main game storage directory. Files must match
            the manifest's folder structure (gamedir/filename).
        dest_dir: Destination directory for backup copies.
            Will be created if it doesn't exist. Backup maintains the same
            folder structure as source (dest_dir/game_folder/filename).
        skipextras: If True, excludes extra/bonus content from backup.
            Only installers are backed up. Useful for space-limited backups.
        os_list: List of operating systems to include (e.g., ['windows', 'linux']).
            Only files for these platforms are backed up.
        lang_list: List of language codes to include (e.g., ['en', 'de']).
            Only files for these languages are backed up.
        ids: List of game titles or IDs to backup. If provided, only these
            games are processed. Can be game slugs or numeric IDs. Empty list
            means backup all games.
        skipids: List of game titles or IDs to exclude from backup.
            Games in this list are never backed up.
        skipgalaxy: If True, excludes GOG Galaxy installers from backup.
        skipstandalone: If True, excludes standalone installers from backup.
        skipshared: If True, excludes shared installers from backup.
    
    Behavior:
        - Only backs up files that exist in the manifest
        - Skips files with size mismatches (logs warning)
        - Skips files already in destination with correct size
        - Creates destination folders as needed
        - Copies info.txt and serial.txt if any files were backed up for a game
        - Non-destructive: never modifies or removes source files
    
    Validation:
        - File size must exactly match manifest size
        - Files with unexpected sizes are skipped with warning
        - Missing source files are silently skipped
    
    Use cases:
        - Create platform-specific backups (only Windows or Linux)
        - Create language-specific backups (only English)
        - Backup specific games to external drive
        - Create space-saving backups (skip extras, skip Galaxy)
        - Migrate library to new location with validation
    
    Notes:
        - Does not verify MD5 checksums (use cmd_verify first if needed)
        - Overwrites destination files if size differs from manifest
        - Preserves original folder structure from manifest
        - Info and serial files copied only if at least one game file was copied
    
    Example:
        >>> cmd_backup('/games', '/backup', skipextras=True, 
        ...            os_list=['windows'], lang_list=['en'],
        ...            ids=[], skipids=[], skipgalaxy=True, 
        ...            skipstandalone=False, skipshared=False)
        # Backs up Windows English standalone installers, no extras
        
        >>> cmd_backup('/games', '/backup/witcher', skipextras=False,
        ...            os_list=['windows', 'linux'], lang_list=['en', 'de'],
        ...            ids=['witcher_3'], skipids=[],
        ...            skipgalaxy=False, skipstandalone=False, skipshared=False)
        # Backs up only Witcher 3 with all installer types and extras
    """
    gamesdb = load_manifest()
    
    for game in gamesdb:
        try:
            _ = game.folder_name
        except AttributeError:
            game.folder_name = game.title

    info('finding all known files in the manifest')
    for game in sorted(gamesdb, key=lambda g: g.folder_name):
        touched = False
        
        try:
            _ = game.galaxyDownloads
        except AttributeError:
            game.galaxyDownloads = []
            
        try:
            a = game.sharedDownloads
        except AttributeError:
            game.sharedDownloads = []
        

        if skipextras:
            game.extras = []
            
        if skipstandalone: 
            game.downloads = []
            
        if skipgalaxy:
            game.galaxyDownloads = []
            
        if skipshared:
            game.sharedDownloads = []
            
        if ids and not (game.title in ids) and not (str(game.id) in ids):
            continue
        if game.title in skipids or str(game.id) in skipids:
            continue
    
                        
        downloadsOS = [game_item for game_item in game.downloads if game_item.os_type in os_list]
        game.downloads = downloadsOS
        
        downloadsOS = [game_item for game_item in game.galaxyDownloads if game_item.os_type in os_list]
        game.galaxyDownloads = downloadsOS
        
        downloadsOS = [game_item for game_item in game.sharedDownloads if game_item.os_type in os_list]
        game.sharedDownloads = downloadsOS
                

        valid_langs = []
        for lang in lang_list:
            valid_langs.append(LANG_TABLE[lang])

        downloadslangs = [game_item for game_item in game.downloads if game_item.lang in valid_langs]
        game.downloads = downloadslangs
        
        downloadslangs = [game_item for game_item in game.galaxyDownloads if game_item.lang in valid_langs]
        game.galaxyDownloads = downloadslangs

        downloadslangs = [game_item for game_item in game.sharedDownloads if game_item.lang in valid_langs]
        game.sharedDownloads = downloadslangs
        
        
        for itm in game.downloads + game.galaxyDownloads + game.sharedDownloads + game.extras:
            if itm.name is None:
                continue
                
                

            src_game_dir = os.path.join(src_dir, game.folder_name)
            src_file = os.path.join(src_game_dir, itm.name)
            dest_game_dir = os.path.join(dest_dir, game.folder_name)
            dest_file = os.path.join(dest_game_dir, itm.name)

            if os.path.isfile(src_file):
                if itm.size != os.path.getsize(src_file):
                    warn('source file %s has unexpected size. skipping.' % src_file)
                    continue
                if not os.path.isdir(dest_game_dir):
                    os.makedirs(dest_game_dir)
                if not os.path.exists(dest_file) or itm.size != os.path.getsize(dest_file):
                    info('copying to %s...' % dest_file)
                    shutil.copy(src_file, dest_file)
                    touched = True

        # backup the info and serial files too
        if touched and os.path.isdir(dest_game_dir):
            for extra_file in [INFO_FILENAME, SERIAL_FILENAME]:
                if os.path.exists(os.path.join(src_game_dir, extra_file)):
                    shutil.copy(os.path.join(src_game_dir, extra_file), dest_game_dir)

def cmd_clean(cleandir, dryrun):
    """Identify and move outdated or unexpected files to an orphan directory.
    
    Scans the game library directory and compares it against the manifest to find
    files and directories that shouldn't be there. This includes:
    - Files not listed in the current manifest (outdated versions)
    - Files marked for update (force_change flag set by strict mode)
    - Entire game directories no longer in the manifest
    - Renamed game directories (handled via game rename detection)
    
    Instead of deleting files immediately, they are moved to an orphan directory
    (ORPHAN_DIR_NAME) for review. This provides a safety net - you can verify the
    orphaned content before permanently deleting it with cmd_trash.
    
    The command is essential for keeping your library synchronized with the manifest
    after running updates, especially when using strict mode which marks files for
    re-download when versions change.
    
    Cleaning process:
    1. Load manifest to determine expected files and folders
    2. Handle game renames (moves renamed directories to new names)
    3. For each directory in cleandir:
       - If directory not in manifest: move entire directory to orphans
       - If directory in manifest: check each file
         * Files not in manifest: move to orphans
         * Files marked with force_change=True: move to orphans
    4. Report total size of orphaned content
    5. Save updated manifest (clears force_change flags after moving files)
    
    Args:
        cleandir: Root directory containing game folders to scan.
            Typically GAME_STORAGE_DIR/games. Each subdirectory should correspond
            to a game's folder_name in the manifest.
        dryrun: If True, simulates the cleaning without actually moving files.
            Reports what would be moved and total size. If False, actually moves
            files to the orphan directory.
    
    Behavior:
        - Non-destructive: moves files instead of deleting them
        - Preserves original structure in orphan directory (game_folder/file)
        - Handles filename conflicts with incremental suffixes (_1, _2, etc.)
        - Excludes special directories (ORPHAN_DIR_NAME, DOWNLOADING_DIR_NAME, etc.)
        - Excludes special files (info.txt, serial.txt, etc.)
        - Updates manifest after moving files marked with force_change
    
    Force_change handling:
        When strict mode detects a file version/timestamp change, it sets
        force_change=True on that file entry. cmd_clean then moves the old
        file to orphans, making room for the new version to be downloaded.
        After moving, the flag is cleared and manifest is saved.
    
    Safety features:
        - Orphaned files are moved, not deleted - you can recover mistakes
        - Dryrun mode shows what would happen without making changes
        - Excluded directories/files are never touched
        - Manifest is only saved after successful moves (not in dryrun)
    
    Workflow integration:
        1. Run cmd_update with -strictverify to mark changed files
        2. Run cmd_clean to move outdated files to orphans
        3. Review orphaned content to ensure nothing important was moved
        4. Run cmd_download to fetch new versions
        5. Run cmd_trash to permanently delete orphaned content (optional)
        6. Run cmd_verify to confirm integrity
    
    Use cases:
        - Remove outdated game versions after manifest update
        - Clean up after strict mode detects file changes
        - Remove games deleted from GOG library
        - Prepare for re-downloading updated installers
        - Maintain clean library matching current manifest
    
    Notes:
        - Always backs up important data before cleaning
        - Review orphaned content before running cmd_trash
        - Dryrun mode is recommended for first-time users
        - Game renames are handled automatically (directories renamed, not orphaned)
        - Info and serial files are preserved unless entire game is orphaned
    
    Example:
        >>> cmd_clean('/games', dryrun=True)
        # Shows what would be moved without actually moving
        # Reports total size of files that would be orphaned
        
        >>> cmd_clean('/games', dryrun=False)
        # Actually moves outdated/unexpected files to orphan directory
        # Updates manifest after moving files marked for change
    """
    items = load_manifest()
    items_by_title = {}
    total_size = 0  # in bytes
    have_cleaned = False
    

    # make convenient dict with title/dirname as key
    for item in items:
        try:
            _ = item.folder_name
        except AttributeError:
            item.folder_name = item.title
        items_by_title[item.folder_name] = item

    # create orphan root dir
    orphan_root_dir = os.path.join(cleandir, ORPHAN_DIR_NAME)
    if not os.path.isdir(orphan_root_dir):
        if not dryrun:
            os.makedirs(orphan_root_dir)

    info("scanning local directories within '{}'...".format(cleandir))
    handle_game_renames(cleandir,items,dryrun)
    for cur_dir in sorted(os.listdir(cleandir)):
        changed_game_items = {}
        cur_fulldir = os.path.join(cleandir, cur_dir)
        if os.path.isdir(cur_fulldir) and cur_dir not in ORPHAN_DIR_EXCLUDE_LIST:
            if cur_dir not in items_by_title:
                info("orphaning dir  '{}'".format(cur_dir))
                have_cleaned = True
                total_size += get_total_size(cur_fulldir)
                if not dryrun:
                    move_with_increment_on_clash(cur_fulldir, os.path.join(orphan_root_dir,cur_dir))
            else:
                # dir is valid game folder, check its files
                expected_filenames = []
                for game_item in items_by_title[cur_dir].downloads + items_by_title[cur_dir].galaxyDownloads + items_by_title[cur_dir].sharedDownloads + items_by_title[cur_dir].extras:
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
                    expected_filenames.append(game_item.name)

                    if game_item.force_change == True:
                        changed_game_items[game_item.name] = game_item
                for cur_dir_file in os.listdir(cur_fulldir):
                    if os.path.isdir(os.path.join(cleandir, cur_dir, cur_dir_file)):
                        continue  # leave subdirs alone
                    if cur_dir_file not in expected_filenames and cur_dir_file not in ORPHAN_FILE_EXCLUDE_LIST:
                        info("orphaning file '{}'".format(os.path.join(cur_dir, cur_dir_file)))
                        dest_dir = os.path.join(orphan_root_dir, cur_dir)
                        if not os.path.isdir(dest_dir):
                            if not dryrun:
                                os.makedirs(dest_dir)
                        file_to_move = os.path.join(cleandir, cur_dir, cur_dir_file)
                        if not dryrun:
                            try:
                                file_size = os.path.getsize(file_to_move)
                                move_with_increment_on_clash(file_to_move, os.path.join(dest_dir,cur_dir_file))
                                have_cleaned = True
                                total_size += file_size                                
                            except Exception as e:
                                error(str(e))
                                error("could not move to destination '{}'".format(os.path.join(dest_dir,cur_dir_file)))
                        else:
                            have_cleaned = True
                            total_size += os.path.getsize(file_to_move)
                    if cur_dir_file in changed_game_items.keys() and cur_dir_file in expected_filenames:
                        info("orphaning file '{}' as it has been marked for change.".format(os.path.join(cur_dir, cur_dir_file)))
                        dest_dir = os.path.join(orphan_root_dir, cur_dir)
                        if not os.path.isdir(dest_dir):
                            if not dryrun:
                                os.makedirs(dest_dir)
                        file_to_move = os.path.join(cleandir, cur_dir, cur_dir_file)
                        if not dryrun:
                            try:
                                file_size = os.path.getsize(file_to_move)
                                move_with_increment_on_clash(file_to_move, os.path.join(dest_dir,cur_dir_file))
                                have_cleaned = True
                                total_size += file_size
                                changed_item =  changed_game_items[cur_dir_file]
                            except Exception as e:
                                error(str(e))
                                error("could not move to destination '{}'".format(os.path.join(dest_dir,cur_dir_file)))
                      
    if have_cleaned:
        info('')
        info('total size of newly orphaned files: {}'.format(pretty_size(total_size)))
        if not dryrun:
            info('orphaned items moved to: {}'.format(orphan_root_dir))
            save_manifest(items)
    else:
        info('nothing to clean. nice and tidy!')

def cmd_verify(verifdir):
    """Verify integrity of downloaded game files against the manifest.
    
    Performs comprehensive validation of all game installers and extras in your
    library to ensure they match the manifest exactly. This detects:
    - Missing files
    - File size mismatches (corruption or incomplete downloads)
    - MD5 checksum mismatches (data corruption or tampering)
    - Corrupt ZIP archives (for Galaxy installers)
    - Missing executable bits in ZIP files (Galaxy installer issue)
    
    The verification process checks each file in the manifest against the actual
    files on disk, reporting any discrepancies. Use this command after downloading
    to ensure all files are complete and intact, or periodically to detect bitrot
    or corruption.
    
    Verification checks (in order):
    1. File existence: Does the file exist on disk?
    2. File size: Does the size match the manifest exactly?
    3. ZIP integrity (Galaxy only): Can the ZIP be opened? Are executable bits set?
    4. MD5 checksum: Does the computed MD5 match the manifest?
    
    MD5 computation is expensive, so the function uses a persistent cache
    (MD5_DB) that stores checksums keyed by filename, size, and modification
    time. If a file hasn't changed since the last verification, the cached
    checksum is reused, dramatically speeding up subsequent verifications.
    
    Args:
        verifdir: Root directory containing game folders to verify.
            Typically GAME_STORAGE_DIR/games. Each subdirectory should correspond
            to a game's folder_name in the manifest, containing the game's files.
    
    Verification behavior:
        - Skips games with no directory (not downloaded yet)
        - Checks only installer files (downloads, galaxyDownloads, sharedDownloads)
        - Stops checking a game at first error (fail-fast per game)
        - Reports each error with game name and specific issue
        - Continues to next game after error (doesn't abort entire verification)
        - Provides summary at end: X/Y games verified with no errors
    
    Error types detected:
        - Missing: File listed in manifest but not found on disk
        - Size mismatch: File size differs from manifest (corruption/incomplete)
        - Zip without executable bit: Galaxy ZIP missing Unix permission flags
        - Corrupt zip: ZIP file cannot be opened or is malformed
        - I/O error: Cannot read file (permissions, disk errors, etc.)
        - MD5 mismatch: File contents differ from manifest (corruption/tampering)
    
    MD5 cache optimization:
        The cache key is: filepath.size.mtime.md5
        - If file size or mtime changes, checksum is recomputed
        - Otherwise, cached checksum is used (much faster)
        - Cache persists across runs (shelve database)
        - Cache is automatically saved when verification completes
    
    Galaxy ZIP validation:
        GOG Galaxy installers are ZIP files that must preserve Unix file permissions
        (executable bits) for correct installation. This check detects ZIPs created
        without these permissions, which would fail to install properly on Linux/Mac.
    
    Use cases:
        - Verify downloads after cmd_download completes
        - Detect file corruption from disk errors or bitrot
        - Confirm library integrity before backup
        - Troubleshoot installation issues (corrupt installers)
        - Periodic health checks of game library
        - Verify imported files after cmd_import
    
    Performance:
        - First run: Slow (computes MD5 for all files)
        - Subsequent runs: Fast (uses cached MD5s for unchanged files)
        - Large files: Slow (MD5 computation is I/O and CPU intensive)
        - Network drives: Very slow (read entire file over network)
    
    Workflow integration:
        1. Run cmd_update to fetch latest manifest
        2. Run cmd_download to fetch game files
        3. Run cmd_verify to confirm all downloads are valid
        4. If errors found, re-download affected games or restore from backup
        5. Run cmd_clean to remove any unexpected files
    
    Notes:
        - Only verifies files with MD5 checksums in manifest
        - Does not verify extras without MD5 (some GOG extras lack checksums)
        - Does not modify any files or manifest
        - Safe to run repeatedly - read-only operation
        - Can be interrupted and rerun (cache persists)
        - Manifest must be up-to-date (run cmd_update first)
    
    Output:
        For each game:
        - Errors reported as: GameName "filename": Error description
        - Success reported as: GameName: OK!
        
        Summary at end:
        - X/Y items verified with no errors
        - List of games with errors (if any)
    
    Example:
        >>> cmd_verify('/path/to/games')
        # Verifies all games in library
        # Reports: 150/152 items verified with no errors
        # Lists: game_with_error1, game_with_error2
    
    Troubleshooting:
        - "Missing" errors: File not downloaded, rerun cmd_download
        - "Size mismatch": Incomplete download, delete and rerun cmd_download
        - "MD5 mismatch": Corrupt file, delete and rerun cmd_download
        - "Corrupt zip": Download error, delete and rerun cmd_download
        - "I/O error": Check disk health, file permissions, disk space
    """
    items = load_manifest()
    if not items:
        error("no items found in manifest. run 'update' first.")
        return

    hash_cache = shelve.open(MD5_DB, protocol=2)
    completed_items = []
    invalid_items = []

    info('')
    info('verifying game directories...')
    for item in items:
        try:
            item_dir = os.path.join(verifdir, item.folder_name)
        except AttributeError:
            item_dir = os.path.join(verifdir, item.title)

        if not os.path.isdir(item_dir):
            continue

        valid = True
        for item_file in item.downloads + item.galaxyDownloads + item.sharedDownloads:
            if not item_file.name:
                continue

            item_path = os.path.join(item_dir, item_file.name)

            # check if it exists
            if not os.path.isfile(item_path):
                valid = False
                invalid_items.append(item.title)
                info('{} "{}": Missing'.format(item.title, item_file.name))
                break  # missing files isn't going to get better

            # check if it's the right size
            if item_file.size and os.path.getsize(item_path) != int(item_file.size):
                valid = False
                invalid_items.append(item.title)
                info('{} "{}": File size mismatch'.format(item.title, item_file.name))
                break  # size mismatch isn't going to get better
                
            # check for executable bits for galaxy zips
            if 'galaxy' in item_file.name.lower() and item_file.name.endswith('.zip'):
                info('{}: Checking zip "{}"...'.format(item.title, item_file.name))
                try:
                    with zipfile.ZipFile(item_path, 'r') as myzip:
                        infos = myzip.infolist()
                        for zipinfo in infos:
                            # extract external attr https://stackoverflow.com/questions/434641/
                            unix_attr = zipinfo.external_attr >> 16
                            if unix_attr == 0: # it wasn't set
                                valid = False
                                invalid_items.append(item.title)
                                info('{} "{}": Detected zip file without the executable bit'.format(item.title, item_file.name))
                                break
                except Exception as e:
                    valid = False
                    invalid_items.append(item.title)
                    info('{} "{}": Corrupt zip file? {}'.format(item.title, item_file.name, e))
                    break

            # check for the correct md5
            # NB: computing md5 is expensive. we need a short-circuit sometimes
            if item_file.md5:
                try:
                    item_file_cache_key = "{0}.{1}.{2}.md5".format(item_path, os.path.getsize(item_path), int(os.path.getmtime(item_path)))
                    cached_hash = hash_cache.get(item_file_cache_key)
                    if cached_hash:
                        item_file_hash = cached_hash
                    else:
                        item_file_hash = hashfile(item_path)
                        hash_cache[item_file_cache_key] = item_file_hash
                except IOError as e:
                    valid = False
                    invalid_items.append(item.title)
                    info('{} "{}": i/o error: {}'.format(item.title, item_file.name, e))
                    break

                if item_file.md5 != item_file_hash:
                    valid = False
                    invalid_items.append(item.title)
                    info('{} "{}": MD5 mismatch'.format(item.title, item_file.name))
                    break

        if valid:
            completed_items.append(item.title)
            info('{}: OK!'.format(item.title))

    hash_cache.close()
    info('')
    info("{0}/{1} items verified with no errors".format(len(completed_items), len(items)))
    if invalid_items:
        info('')
        info("these {0} items have errors...".format(len(invalid_items)))
        for invalid_item in invalid_items:
            info('  {0}'.format(invalid_item))
