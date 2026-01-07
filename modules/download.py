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
from .utils import html2text

# Utility functions for size formatting
def megs(b):
    """Format bytes as megabytes"""
    return '%.1fMB' % (b / float(1024**2))

def gigs(b):
    """Format bytes as gigabytes"""
    return '%.2fGB' % (b / float(1024**3))

def filter_games_by_id(items, ids, skipids):
    """Filter game items by ids and skipids, with error handling"""
    if ids:
        formattedIds = ', '.join(map(str, ids))
        info("downloading games with id(s): {%s}" % formattedIds)
        if items:
            info("First item title: '{}', id: '{}'".format(
                items[0].title if hasattr(items[0], 'title') else 'NO TITLE', 
                items[0].id if hasattr(items[0], 'id') else 'NO ID'))
        items = [item for item in items if item.title in ids or str(item.id) in ids]
        info("Filtered to {} games".format(len(items)))

    if skipids:
        formattedSkipIds = ', '.join(map(str, skipids))
        info("skipping games with id(s): {%s}" % formattedSkipIds)
        items = [item for item in items if item.title not in skipids and str(item.id) not in skipids]

    if not items:
        if ids and skipids:
            error('no game(s) with id(s) in "{}" was found'.format(ids) + 
                  'after skipping game(s) with id(s) in "{}".'.format(skipids))
        elif ids:
            error('no game with id in "{}" was found.'.format(ids))
        elif skipids:
            error('no game was found was found after skipping game(s) with id(s) in "{}".'.format(skipids))
        else:
            error('no game found')
        exit(1)
    
    return items

def filter_downloads_by_os_and_lang(downloads, os_list, lang_list):
    """Filter download items by OS and language.
    
    Note: This should only be used for game installers (downloads/galaxyDownloads/sharedDownloads),
    not for extras. Extras have os_type='extra' and lang='' and should not be filtered by OS/language.
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
    """Clean up a temporary directory by removing outdated directories and files.
    
    Args:
        target_dir: The directory to clean up
        all_items_by_title: Dictionary mapping game titles to their manifest items
        dryrun: If True, only report what would be done without making changes
        skip_subdir: Optional subdirectory name to skip (e.g., PROVISIONAL_DIR_NAME)
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
    """Write game information to an info text file.
    
    Args:
        item_homedir: The game's home directory
        item: The game manifest item
        filtered_downloads: List of filtered standalone installers
        filtered_galaxyDownloads: List of filtered Galaxy installers
        filtered_sharedDownloads: List of filtered shared installers
        filtered_extras: List of extras (not filtered)
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
    """Download a single image from an item's key field.
    
    Args:
        item: The game manifest item
        key: The attribute name containing the image URL (e.g., 'bg_url', 'image_url')
        images_dir_name: Directory to save images
        image_orphandir: Directory to move old images
        clean_existing: If True, move old files to orphan dir; if False, delete them
        downloadSession: The download session to use for requests
    """
    images_key_dir_name = os.path.join(images_dir_name, key)
    key_local_path = item[key].lstrip("/") + ".jpg"
    key_url = 'https://' + key_local_path
    (dir, file) = os.path.split(key_local_path)
    key_local_path_dir = os.path.join(images_key_dir_name, dir) 
    key_local_path_file = os.path.join(key_local_path_dir, file) 
    modified_images_key_dir_name = images_key_dir_name
    
    if (platform.system() == "Windows" and sys.version_info[0] < 3):
        key_local_path_file = uLongPathPrefix + os.path.abspath(key_local_path_file)
        key_local_path_dir = uLongPathPrefix + os.path.abspath(key_local_path_dir)
        image_orphandir = uLongPathPrefix + os.path.abspath(image_orphandir)
        modified_images_key_dir_name = uLongPathPrefix + os.path.abspath(modified_images_key_dir_name)
        
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
    """Download multiple images from an item's keys dictionary field.
    
    Args:
        item: The game manifest item
        keys: The attribute name containing a dictionary of image URLs (e.g., 'bg_urls')
        images_dir_name: Directory to save images
        image_orphandir: Directory to move old images
        clean_existing: If True, move old files to orphan dir; if False, delete them
        downloadSession: The download session to use for requests
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
        
        if (platform.system() == "Windows" and sys.version_info[0] < 3):
            longpath_safe_leading_partial_key_local_path_dir = uLongPathPrefix + os.path.abspath(longpath_safe_leading_partial_key_local_path_dir)
            
        if not os.path.exists(longpath_safe_leading_partial_key_local_path_dir):
            os.makedirs(longpath_safe_leading_partial_key_local_path_dir)
            
        full_key_local_path_dir = os.path.join(leading_partial_key_local_path_dir, trailing_partial_key_local_path_dir)
        full_key_local_path_file = os.path.join(full_key_local_path_dir, trailing_partial_key_local_path_file)
        key_url = 'https://' + partial_key_local_path
        
        if (platform.system() == "Windows" and sys.version_info[0] < 3):
            full_key_local_path_file = uLongPathPrefix + os.path.abspath(full_key_local_path_file)
            full_key_local_path_dir = uLongPathPrefix + os.path.abspath(full_key_local_path_dir)
            
        if not os.path.exists(full_key_local_path_file):
            if os.path.exists(full_key_local_path_dir):
                images_full_key_local_path_orphandir = os.path.join(images_key_orphandir_name, leading_partial_key_local_path)
                if (platform.system() == "Windows" and sys.version_info[0] < 3):
                    images_full_key_local_path_orphandir = uLongPathPrefix + os.path.abspath(images_full_key_local_path_orphandir)
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
                if (platform.system() == "Windows" and sys.version_info[0] < 3):
                    potential_old_folder_path = uLongPathPrefix + os.path.abspath(potential_old_folder_path)
                    images_key_orphandir_name = uLongPathPrefix + os.path.abspath(images_key_orphandir_name)
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
    """Preallocate disk space for a file to improve download performance.
    
    Args:
        file_path: Path to the file to preallocate
        target_size: Target size in bytes
        skip_preallocation: If True, skip preallocation entirely
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
    """Download a single chunk of a file with retry logic.
    
    Returns:
        tuple: (success: bool, actual_size: int or None) - Returns actual size if manifest mismatch detected
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
    """Download a file using MD5 chunk verification.
    
    Returns:
        tuple: (success: bool, actual_size: int or None) - Returns actual size if manifest mismatch detected
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
    """Download a file without chunk verification (simple single-request download).
    
    Returns:
        tuple: (success: bool, actual_size: int or None) - Returns actual size if manifest mismatch detected
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
    """Download content from response and write to file, tracking progress.
    
    Args:
        tid: Thread ID
        path: File path (for error messages and tracking)
        response: HTTP response object with iter_content()
        out: Output file handle
        sizes: Dictionary tracking remaining bytes per file
        lock: Threading lock for synchronized access
        rates: Dictionary tracking download rates
        
    Returns:
        int: Number of bytes downloaded
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
    """Display progress lines with clean terminal rewriting.
    
    Args:
        progress_lines: List of strings to display
        last_line_count: List containing count of previously displayed lines (mutable)
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
    items = filter_games_by_id(items, ids, skipids)

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

    
    for dir in os.listdir(downloading_root_dir):
        if dir != PROVISIONAL_DIR_NAME:
            testdir= os.path.join(downloading_root_dir,dir)
            if os.path.isdir(testdir):
                if not os.listdir(testdir):
                    try:
                        os.rmdir(testdir)
                    except Exception:
                        pass

    for dir in os.listdir(provisional_root_dir):
        testdir= os.path.join(provisional_root_dir,dir)
        if os.path.isdir(testdir):
            if not os.listdir(testdir):
                try:
                    os.rmdir(testdir)
                except Exception:
                    pass
                    
