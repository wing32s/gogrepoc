#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import datetime
import copy
import sys
import pprint
import requests

from .utils import (
    AttrDict, info, warn, error, debug, log_exception,
    MANIFEST_FILENAME, RESUME_MANIFEST_FILENAME, CONFIG_FILENAME,
    MD5_DIR_NAME, RESUME_MANIFEST_SYNTAX_VERSION,
    ORPHAN_DIR_NAME, GOG_HOME_URL, LANG_TABLE,
    move_with_increment_on_clash
)
from .api import fetch_file_info

def load_manifest(filepath=MANIFEST_FILENAME, ignore_locks=False):
    info('loading manifest...')
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as r:
                ad = r.read().replace('{', 'AttrDict(**{').replace('}', '})')
            result = eval(ad)
            info('manifest loaded successfully with {} items'.format(len(result)))
            return result
        except Exception as e:
            error("failed to parse manifest: {}".format(str(e)))
            import traceback
            traceback.print_exc()
            sys.exit(1)
    else:
        info('manifest file not found at {}'.format(filepath))
        return []

def save_manifest(items, filepath=MANIFEST_FILENAME, update_md5_xml=False, delete_md5_xml=False):
    info('saving manifest...')
    try:
        with open(filepath, 'w', encoding='utf-8') as w:
            w.write('# GOGRepo Manifest %s\n' % datetime.date.today())
            pprint.pprint(items, width=123, stream=w)
            
        if update_md5_xml:
            if not os.path.exists(MD5_DIR_NAME):
                os.makedirs(MD5_DIR_NAME)
                
            for item in items:
                try: 
                    _ = item.gog_data.md5_xml.text
                except AttributeError:
                    continue  

                if (item.gog_data.md5_xml.text is not None):
                    with open(os.path.join(MD5_DIR_NAME,item.title+".xml"), 'w', encoding='utf-8') as w:
                        w.write(item.gog_data.md5_xml.text)
                    if delete_md5_xml:
                         item.gog_data.md5_xml.text = None   
                         
                    #Too large need a better way to handle this
                    #for chunk in item.gog_data.md5_xml.chunks:
                        # try:
                        #     with open(os.path.join(MD5_DIR_NAME, item.gog_data.md5_xml.chunks[chunk].tag + ".xml"), 'w') as w:
                        #         w.write(item.gog_data.md5_xml.chunks[chunk].text)
                        #     if delete_md5_xml:
                        #         item.gog_data.md5_xml.chunks[chunk].text = None        
                        # except Exception as e:
                        #      pass
            if delete_md5_xml: #Do this again to make sure all memory is freed even if we crash part way through writting
                for item in items:
                    try:
                        item.gog_data.md5_xml.text = None
                        #item.gog_data.md5_xml.chunks = {}
                    except AttributeError:
                        pass
        info('saved manifest')
    except KeyboardInterrupt:
        #If we ctrl-c whilst saving simply try again.
        with open(filepath, 'w', encoding='utf-8') as w:
            w.write('# GOGRepo Manifest %s\n' % datetime.date.today())
            pprint.pprint(items, width=123, stream=w)
        info('saved manifest') 
        raise

def load_resume_manifest(filepath=RESUME_MANIFEST_FILENAME):
    info('loading resume manifest...')
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as r:
                ad = r.read().replace('{', 'AttrDict(**{').replace('}', '})')
            return eval(ad)
        except Exception:
            return []
    else:
        return []

def save_resume_manifest(items, filepath=RESUME_MANIFEST_FILENAME):
    info('saving resume manifest...')
    try:
        with open(filepath, 'w', encoding='utf-8') as w:
            pprint.pprint(items, width=123, stream=w)
            info('saved resume manifest')
    except KeyboardInterrupt:
        with open(filepath, 'w', encoding='utf-8') as w:
            pprint.pprint(items, width=123, stream=w)
        info('saved resume manifest')    
        raise

def item_checkdb(search_item, db):
    for i, item in enumerate(db):
        if item.id == search_item:
            return i
    return None

def load_config_file(filepath=CONFIG_FILENAME):
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as r:
                # support comments in the config file
                clean_lines = []
                for line in r:
                    if not line.strip().startswith('#'):
                        clean_lines.append(line)
                ad = ''.join(clean_lines).replace('{', 'AttrDict(**{').replace('}', '})')
            return eval(ad)
        except Exception:
            warn("failed to parse config file")
            return AttrDict()
    else:
        return AttrDict()

def save_config_file(config, filepath=CONFIG_FILENAME):
    try:
        with open(filepath, 'w', encoding='utf-8') as w:
            pprint.pprint(config, width=123, stream=w)
    except KeyboardInterrupt:
        with open(filepath, 'w', encoding='utf-8') as w:
            pprint.pprint(config, width=123, stream=w)
        raise

def deDuplicateList(duplicatedList, existingItems, strictDupe):   
    deDuplicatedList = []
    for update_item in duplicatedList:
        if update_item.name is not None:                
            dummy_item = copy.copy(update_item)
            deDuplicatedName = deDuplicateName(dummy_item, existingItems, strictDupe)
            if deDuplicatedName is not None:
                if (update_item.name != deDuplicatedName):
                    info('  -> ' + update_item.name + ' already exists in this game entry with a different size and/or md5, this file renamed to ' + deDuplicatedName)                        
                    update_item.name = deDuplicatedName
                deDuplicatedList.append(update_item)
            else:
                info('  -> ' + update_item.name + ' already exists in this game entry with same size/md5, skipping adding this file to the manifest') 
        else: 
            #Placeholder for an item coming soon, pass through
            deDuplicatedList.append(update_item)
    return deDuplicatedList        
        
def deDuplicateName(potentialItem, clashDict, strictDupe):
    try: 
        #Check if Name Exists
        existingDict = clashDict[potentialItem.name] 
        try:
            #Check if this md5 / size pair have already been resolved
            prevItemsCount = 0
            for key in existingDict:
                prevItemsCount += len(existingDict[key]) 
            md5list = existingDict[potentialItem.size]
            try:
                idx = md5list.index(potentialItem.md5)
            except ValueError:
                #Do this early, so we can abort early if need to rely on size match.
                existingDict[potentialItem.size].append(potentialItem.md5) #Mark as resolved
                if ((not strictDupe) and (None in md5list or potentialItem.md5 == None)):
                    return None
                else:
                    potentialItem.name = makeDeDuplicateName(potentialItem.name, prevItemsCount)
                    return deDuplicateName(potentialItem, clashDict, strictDupe)
            return None
        except KeyError:
            potentialItem.name = makeDeDuplicateName(potentialItem.name, prevItemsCount)
            existingDict[potentialItem.size] = [potentialItem.md5] #Mark as resolved
            return deDuplicateName(potentialItem, clashDict, strictDupe)
    except KeyError:
        #No Name Clash
        clashDict[potentialItem.name] = {potentialItem.size:[potentialItem.md5]}
        return potentialItem.name

def makeDeDuplicateName(name, prevItemsCount):
    # Handle files without extensions
    if os.extsep not in name:
        return name + "(" + str(prevItemsCount) + ")"
    
    root, ext = name.rsplit(os.extsep, 1) #expand this to cover eg tar.zip
    ext = os.extsep + ext
    if (ext != ".bin"):
        name = root + "("+str(prevItemsCount) + ")" + ext
    else:
        #bin file, adjust name to account for gogs weird extension method
        setDelimiter = root.rfind("-")
        try:
            setPart = int(root[setDelimiter+1:])
        except ValueError:
            #This indicators a false positive. The "-" found was part of the file name not a set delimiter. 
            setDelimiter = -1 
        if (setDelimiter == -1):
            #not part of a bin file set , some other binary file , treat it like a non .bin file
            name = root + "("+str(prevItemsCount) + ")" + ext
        else:    
            name = root[:setDelimiter] + "("+str(prevItemsCount) + ")" + root[setDelimiter:] + ext
    return name

def handle_game_renames(savedir, gamesdb, dryrun):   
    info("scanning manifest for renames...")
    orphan_root_dir = os.path.join(savedir, ORPHAN_DIR_NAME)
    if not os.path.isdir(orphan_root_dir):
        os.makedirs(orphan_root_dir)

    for game in gamesdb:
        try:
            _ = game.galaxyDownloads
        except AttributeError:
            game.galaxyDownloads = []
            
        try:
            a = game.sharedDownloads
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
        if (game.old_folder_name is not None):
            src_dir = os.path.join(savedir, game.old_folder_name)
            dst_dir = os.path.join(savedir, game.folder_name)   
            if os.path.isdir(src_dir):
                try:
                    if os.path.exists(dst_dir):
                        warn("orphaning destination clash '{}'".format(dst_dir))
                        if not dryrun:
                            move_with_increment_on_clash(dst_dir, os.path.join(orphan_root_dir,game.folder_name))
                    info('  -> renaming directory "{}" -> "{}"'.format(src_dir, dst_dir))            
                    if not dryrun:                    
                        move_with_increment_on_clash(src_dir,dst_dir)
                except Exception: 
                    error('    -> rename failed "{}" -> "{}"'.format(game.old_folder_name, game.folder_name))
        for item in game.downloads+game.galaxyDownloads+game.sharedDownloads+game.extras:
            try: 
                _ = item.old_name 
            except AttributeError:
                item.old_name = None
        
            if (item.old_name is not None):            
                game_dir =  os.path.join(savedir, game.folder_name)
                src_file =  os.path.join(game_dir,item.old_name)
                dst_file =  os.path.join(game_dir,item.name)
                if os.path.isfile(src_file):
                    try:
                        if os.path.exists(dst_file):
                            warn("orphaning destination clash '{}'".format(dst_file))
                            dest_dir = os.path.join(orphan_root_dir, game.folder_name)
                            if not os.path.isdir(dest_dir):
                                if not dryrun:
                                    os.makedirs(dest_dir)
                            if not dryrun:
                                move_with_increment_on_clash(dst_file, os.path.join(dest_dir,item.name))
                        info('  -> renaming file "{}" -> "{}"'.format(src_file, dst_file))
                        if not dryrun:
                            move_with_increment_on_clash(src_file,dst_file)
                            item.old_name = None #only once
                    except Exception:
                        error('    -> rename failed "{}" -> "{}"'.format(src_file, dst_file))
                        if not dryrun:
                            item.prev_verified = False

def handle_game_updates(olditem, newitem,strict, update_downloads_strict, update_extras_strict):
    try:
        _ = olditem.galaxyDownloads
    except AttributeError:
        olditem.galaxyDownloads = []
        
    try:
        a = olditem.sharedDownloads
    except AttributeError:
        olditem.sharedDownloads = []
    try:
        a = olditem.folder_name
    except AttributeError:
        olditem.folder_name = olditem.title
    try:
        a = newitem.folder_name
    except AttributeError:
        newitem.folder_name = newitem.title

    if newitem.has_updates:
        info('  -> gog flagged this game as updated')

    if olditem.title != newitem.title:
        info('  -> title has changed "{}" -> "{}"'.format(olditem.title, newitem.title))
        newitem.old_title = olditem.title

    if olditem.folder_name != newitem.folder_name:
        info('  -> folder name has changed "{}" -> "{}"'.format(olditem.folder_name, newitem.folder_name))
        newitem.old_folder_name = olditem.folder_name

    if olditem.long_title != newitem.long_title:
        try:
            info('  -> long title has change "{}" -> "{}"'.format(olditem.long_title, newitem.long_title))
        except UnicodeEncodeError:
            pass

    if olditem.changelog != newitem.changelog and newitem.changelog not in [None, '']:
        info('  -> changelog was updated')

    try:
        if olditem.serials != newitem.serials:
            info('  -> serial key(s) have changed')
    except AttributeError:
        if olditem.serial != '':
            info('  -> gogrepoc serial key format has changed')
        if olditem.serial != newitem.serial:
            info('  -> serial key has changed')
                    
    #Done this way for backwards compatability. Would be faster to do each separately.     
    for newDownload in newitem.downloads+newitem.galaxyDownloads+newitem.sharedDownloads:
        candidate = None
        for oldDownload in olditem.downloads+olditem.galaxyDownloads+olditem.sharedDownloads:
            if oldDownload.md5 is not None:
                if oldDownload.md5 == newDownload.md5 and oldDownload.size == newDownload.size and oldDownload.lang == newDownload.lang:
                    if oldDownload.name == newDownload.name:
                        candidate = oldDownload #Match already exists
                        break #Can't be overriden so end it now
                    if oldDownload.name != newDownload.name and ( candidate == None or candidate.md5 == None ) : #Will not override and gets overridden by a perfect match (also allows only one match)
                         candidate = oldDownload
            else:            
                if oldDownload.size == newDownload.size and oldDownload.name == newDownload.name and oldDownload.lang == newDownload.lang and candidate == None:
                    candidate = AttrDict(**oldDownload.copy())
                    if strict:
                        try:
                           candidate.prev_verified = False        
                        except AttributeError:
                            pass
        if candidate != None:
            try: 
                _ = candidate.unreleased
            except AttributeError:
                candidate.unreleased = False
            try:
                newDownload.prev_verified = candidate.prev_verified         
            except AttributeError:
                newDownload.prev_verified = False
            try:
                newDownload.old_updated = candidate.old_updated #Propogate until actually updated.
            except AttributeError:
                newDownload.old_updated = None
            try:
                newDownload.force_change = candidate.force_change
            except AttributeError:
                newDownload.force_change = False #An entry lacking force_change will also lack old_updated so this gets handled later 
          
            oldUpdateTime = None
            updateTime = None
            if newDownload.old_updated is not None:
                oldUpdateTime = datetime.datetime.fromisoformat(newDownload.old_updated)
            if newDownload.updated is not None:
                updateTime = datetime.datetime.fromisoformat(newDownload.updated)
            newestUpdateTime = None
            newer = False
            if updateTime is None:
                newer = True  #Treating this as definitive because it's probably a result of an item being removed
            elif oldUpdateTime is None:
                newestUpdateTime = newDownload.updated
            elif updateTime > oldUpdateTime:
                newer = True
                newestUpdateTime = newDownload.updated
            else:
                newestUpdateTime = newDownload.old_updated

            if candidate.name != newDownload.name:
                info('  -> in folder_name "{}" a download has changed name "{}" -> "{}"'.format(newitem.folder_name,candidate.name,newDownload.name))
                newDownload.old_name  = candidate.name
            if (candidate.md5 != None and candidate.md5 == newDownload.md5 and candidate.size == newDownload.size) or ( newDownload.unreleased and candidate.unreleased ):
                #Not released or MD5s match , so whatever the update was it doesn't matter
                newDownload.old_updated = newestUpdateTime
                newDownload.updated =  newestUpdateTime
            elif update_downloads_strict: 
                newDownload.updated = newestUpdateTime #Don't forget our *newest* update time.
                if newer:
                    info('  -> in folder_name "{}" a download "{}" has probably been updated (update date {} -> {}) and has been marked for change."'.format(newitem.folder_name,newDownload.name,newDownload.old_updated,newDownload.updated))
                    newDownload.force_change = True
        else:
            #New file entry, presume changed 
            newDownload.force_change = True
                
    for newExtra in newitem.extras: 
        candidate = None
        for oldExtra in olditem.extras:                    
            if (oldExtra.md5 != None):                
                if oldExtra.md5 == oldExtra.md5 and oldExtra.size == newExtra.size:
                    if oldExtra.name == newExtra.name:
                        candidate = oldExtra #Match already exists
                        break #Can't be overriden so end it now
                    if oldExtra.name != newExtra.name and (candidate == None or candidate.md5 == None):
                        candidate = oldExtra
            else:    
                if oldExtra.name == newExtra.name and oldExtra.size == newExtra.size and candidate == None:
                    candidate = AttrDict(**oldExtra.copy())
                    if strict:
                        try:
                            #candidate.force_change = True
                            candidate.prev_verified = False
                        except AttributeError:
                            pass
        if candidate != None:
            try: 
                _ = candidate.unreleased
            except AttributeError:
                candidate.unreleased = False
            try:
                newExtra.prev_verified = candidate.prev_verified         
            except AttributeError:
                newExtra.prev_verified = False
            try:
                newExtra.force_change = candidate.force_change
            except AttributeError:
                newExtra.force_change = False #An entry lacking force_change will also lack old_updated so this gets handled later
            try:
                newExtra.old_updated = candidate.old_updated #Propogate until actually updated.
            except AttributeError:
                newExtra.old_updated = None

            oldUpdateTime = None
            updateTime = None
            if newExtra.old_updated is not None:
                oldUpdateTime = datetime.datetime.fromisoformat(newExtra.old_updated)
            if newExtra.updated is not None:
                updateTime = datetime.datetime.fromisoformat(newExtra.updated)
            newestUpdateTime = None
            newer = False
            if updateTime is None:
                newer = True  #Treating this as definitive because it's probably a result of an item being removed
            elif oldUpdateTime is None:
                newestUpdateTime = newExtra.updated
            elif updateTime > oldUpdateTime:
                newer = True
                newestUpdateTime = newExtra.updated
            else:
                newestUpdateTime = newExtra.old_updated
                
            if candidate.name != newExtra.name:
                info('  -> in folder_name "{}" an extra has changed name "{}" -> "{}"'.format(newitem.folder_name,candidate.name,newExtra.name))
                newExtra.old_name  = candidate.name
            if (candidate.md5 != None and candidate.md5 == newExtra.md5 and candidate.size == newExtra.size) or ( newExtra.unreleased and candidate.unreleased ):
                #Not released or MD5s match , so whatever the update was it doesn't matter
                newExtra.old_updated = newestUpdateTime
                newExtra.updated =  newestUpdateTime
            elif update_extras_strict: 
                newExtra.updated = newestUpdateTime #Don't forget our *newest* update time.
                if newer:
                    info('  -> in folder_name "{}" an extra "{}" has perhaps been updated (update date {} -> {}) and has been marked for change."'.format(newitem.folder_name,newExtra.name,newExtra.old_updated,newExtra.updated))
                    newExtra.force_change = True
        else:
            #New file entry, presume changed 
            newExtra.force_change = True

def filter_downloads(out_list, downloads_list, lang_list, os_list,save_md5_xml,updateSession):
    """filters any downloads information against matching lang and os, translates
    them, and extends them into out_list
    """
    filtered_downloads = []
    downloads_dict = dict(downloads_list)

    # hold list of valid languages languages as known by gogapi json stuff
    valid_langs = []
    for lang in lang_list:
        valid_langs.append(LANG_TABLE[lang])

    # check if lang/os combo passes the specified filter
    for lang in downloads_dict:
        if lang in valid_langs:
            for os_type in downloads_dict[lang]:
                if os_type in os_list:
                    for download in downloads_dict[lang][os_type]:
                        tempd = download['manualUrl']
                        if tempd[:10] == "/downloads":
                            tempd = "/downlink" +tempd[10:]
                        hrefs = [GOG_HOME_URL + download['manualUrl'],GOG_HOME_URL + tempd]
                        href_ds = []
                        file_info_success = False
                        md5_success = False
                        unreleased = False
                        for href in hrefs:
                            if not (unreleased or (file_info_success and md5_success)):
                                debug("trying to fetch file info from %s" % href)
                                file_info_success = False
                                md5_success = False
                                # passed the filter, create the entry
                                d = AttrDict(desc=download['name'],
                                             os_type=os_type,
                                             lang=lang,
                                             version=download['version'],
                                             href= href,
                                             md5=None,
                                             name=None,
                                             size=None,
                                             prev_verified=False,
                                             old_name=None,
                                             unreleased = False,
                                             md5_exempt = False,
                                             gog_data = AttrDict(),
                                             updated = None,
                                             old_updated = None,
                                             force_change = False,
                                             old_force_change = None
                                             )
                                for key in download:
                                    try:
                                        tmp_contents = d[key]
                                        if tmp_contents != download[key]:
                                            debug("GOG Data Key, %s , for download clashes with Download Data Key storing detailed info in secondary dict" % key)
                                            d.gog_data[key] = download[key]
                                    except Exception:
                                        d[key] = download[key]             
                                if d.gog_data.size == "0 MB":#Not Available
                                    warn("Unreleased File, Skipping Data Fetching %s" % d.desc)
                                    d.unreleased = True
                                    unreleased = True
                                else: #Available
                                    try:
                                        fetch_file_info(d, True,save_md5_xml,updateSession)
                                        file_info_success = True
                                    except requests.HTTPError:
                                        warn("failed to fetch %s" % (d.href))
                                    except Exception:
                                        warn("failed to fetch %s and because of non-HTTP Error" % (d.href))
                                        warn("The handled exception was:")
                                        log_exception('')
                                        warn("End exception report.")
                                    if d.md5_exempt == True or d.md5 != None:
                                        md5_success = True
  
                                    
                                href_ds.append([d,file_info_success,md5_success])
                        if unreleased:
                            debug("File Not Available For Manual Download Storing Canonical Link: %s" % d.href)
                            filtered_downloads.append(d)
                        elif file_info_success and md5_success: #Will be the current d because no more are created once we're successful
                            debug("Successfully fetched file info and md5 from %s" % d.href)
                            filtered_downloads.append(d)
                        else: #Check for first file info success since all MD5s failed.
                            any_file_info_success = False
                            for href_d in href_ds:
                                if not any_file_info_success:
                                    if (href_d[1]) == True:
                                        any_file_info_success = True
                                        filtered_downloads.append(href_d[0])
                                        debug("Successfully fetched file info from %s but no md5 data was available" % href_d[0].href)
                            if not any_file_info_success:
                                #None worked so go with the canonical link
                                error("Could not fetch file info so using canonical link: %s" % href_ds[0][0].href)
                                filtered_downloads.append(href_ds[0][0])
    out_list.extend(filtered_downloads)

def filter_extras(out_list, extras_list,save_md5_xml,updateSession):
    """filters and translates extras information and adds them into out_list
    """
    filtered_extras = []

    for extra in extras_list:
        tempd = extra['manualUrl']
        if tempd[:10] == "/downloads":
            tempd = "/downlink" +tempd[10:]
        hrefs = [GOG_HOME_URL + extra['manualUrl'],GOG_HOME_URL + tempd]
        href_ds = []
        file_info_success = False
        unreleased = False
        for href in hrefs:
            if not (unreleased or file_info_success):
                debug("trying to fetch file info from %s" % href)
                file_info_success = False
                d = AttrDict(desc=extra['name'],
                             os_type='extra',
                             lang='',
                             version=None,
                             href= href,
                             md5=None,
                             name=None,
                             size=None,
                             prev_verified=False,
                             old_name = None,
                             unreleased = False,
                             gog_data = AttrDict(),
                             updated = None,
                             old_updated = None,
                             force_change = False,
                             old_force_change = None
                             )
                for key in extra:
                    try:
                        tmp_contents = d[key]
                        if tmp_contents != extra[key]:
                            debug("GOG Data Key, %s , for extra clashes with Extra Data Key storing detailed info in secondary dict" % key)
                            d.gog_data[key] = extra[key]
                    except Exception:
                        d[key] = extra[key]
                if d.gog_data.size == "0 MB":#Not Available
                    debug("Unreleased File, Skipping Data Fetching %s" % d.desc)
                    d.unreleased = True
                    unreleased = True
                else:
                    try:
                        fetch_file_info(d, False,save_md5_xml,updateSession)
                        file_info_success = True
                    except requests.HTTPError:
                        warn("failed to fetch %s" % d.href)
                    except Exception:
                        warn("failed to fetch %s because of non-HTTP Error" % d.href)
                        warn("The handled exception was:")
                        log_exception('')
                        warn("End exception report.")
                href_ds.append([d,file_info_success])
        if unreleased:
            debug("File Not Available For Manual Download Storing Canonical Link: %s" % d.href)
            filtered_extras.append(d)
        elif file_info_success: #Will be the current d because no more are created once we're successful
            debug("Successfully fetched file info from %s" % d.href)
            filtered_extras.append(d)
        else:
            #None worked so go with the canonical link
            error("Could not fetch file info so using canonical link: %s" % href_ds[0][0].href)
            filtered_extras.append(href_ds[0][0])
    out_list.extend(filtered_extras)

def filter_dlcs(item, dlc_list, lang_list, os_list,save_md5_xml,updateSession):
    """filters any downloads/extras information against matching lang and os, translates
    them, and adds them to the item downloads/extras

    dlcs can contain dlcs in a recursive fashion, and oddly GOG does do this for some titles.
    """
    from urllib.parse import urlparse
    for dlc_dict in dlc_list:
        base_title = dlc_dict['title']
        potential_title = base_title
        i = 1
        while potential_title in item.used_titles:
            potential_title = base_title + " (" + str(i) + ")"
            i = i + 1
        item.used_titles.append(potential_title)
        if urlparse(dlc_dict['backgroundImage']).path != "":
           item.bg_urls[potential_title] = dlc_dict['backgroundImage']
        if dlc_dict['cdKey'] != '':
            item.serials[potential_title] = dlc_dict['cdKey']
            # Modernized logic: assumes Python 3
            if (not(item.serials[potential_title].isprintable())): #Probably encoded in UTF-16
                pserial = item.serials[potential_title]
                if (len(pserial) % 2): #0dd
                    pserial=pserial+"\x00" 
                try:
                    pserial = bytes(pserial,"UTF-8")
                    pserial = pserial.decode("UTF-16")
                    if pserial.isprintable():
                        item.serials[potential_title] = pserial
                    else:
                        warn('DLC serial code is unprintable for %s, storing raw',potential_title)
                except Exception:
                    warn('DLC serial code is unprintable and decoding failed for %s, storing raw',potential_title)

        filter_downloads(item.downloads, dlc_dict['downloads'], lang_list, os_list,save_md5_xml,updateSession)
        filter_downloads(item.galaxyDownloads, dlc_dict['galaxyDownloads'], lang_list, os_list,save_md5_xml,updateSession)
        filter_extras(item.extras, dlc_dict['extras'],save_md5_xml,updateSession)
        filter_dlcs(item, dlc_dict['dlcs'], lang_list, os_list,save_md5_xml,updateSession)  # recursive
