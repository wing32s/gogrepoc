#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import time
import datetime
import logging
import threading
import shutil
import zipfile
import re
import getpass
import html5lib
import xml.etree.ElementTree
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
    move_with_increment_on_clash, pretty_size, get_total_size,
    HTTP_RETRY_DELAY, HTTP_GAME_DOWNLOADER_THREADS, HTTP_TIMEOUT,
    MANIFEST_FILENAME, RESUME_MANIFEST_FILENAME, CONFIG_FILENAME,
    MD5_DIR_NAME, DOWNLOADING_DIR_NAME, PROVISIONAL_DIR_NAME,
    ORPHAN_DIR_NAME, IMAGES_DIR_NAME, INFO_FILENAME, SERIAL_FILENAME,
    GAME_STORAGE_DIR, RESUME_MANIFEST_SYNTAX_VERSION, RESUME_SAVE_THRESHOLD,
    GOG_HOME_URL, GOG_LOGIN_URL, GOG_AUTH_URL, GOG_TOKEN_URL,
    GOG_GALAXY_REDIRECT_URL, GOG_CLIENT_ID, GOG_SECRET,
    GOG_MEDIA_TYPE_GAME, GOG_MEDIA_TYPE_MOVIE, GOG_ACCOUNT_URL,
    REPO_HOME_URL, NEW_RELEASE_URL, LANG_TABLE, SKIP_MD5_FILE_EXT,
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

# For preallocation
GENERIC_READ = 0x80000000
GENERIC_WRITE = 0x40000000
CREATE_NEW = 0x1    
OPEN_EXISTING = 0x3
FILE_BEGIN = 0x0
# Ideally move these to utils or cross-platform shim, but keeping here for now as they are used in cmd_download

lock = threading.Lock()

def cmd_login(username, password, user_id=None):
    """Attempts to log into GOG Galaxy API and saves the resulting Token to disk."""
    
    # Prompt for login/password if needed
    if username is None or password is None:
        info("You must use a GOG or GOG Galaxy account, Google/Discord sign-ins are not currently supported.")
    if username is None:
        username = input("Username: ")
    if password is None:
        password = getpass.getpass()
    
    # Use the exact same redirect_uri for both authorization and token requests
    redirect_uri = GOG_GALAXY_REDIRECT_URL + '?origin=client'
    
    token_data = {
        'user': username,
        'passwd': password,
        'login_token': None,
        'totp_url': None,
        'totp_token': None,
        'two_step_url': None,
        'two_step_token': None,
        'login_code': None
    }
    
    loginSession = makeGOGSession(loginSession=True)
    
    # Fetch the auth url
    info("attempting Galaxy login as '{}' ...".format(token_data['user']))
    
    page_response = request(loginSession, GOG_AUTH_URL, 
                           args={'client_id': GOG_CLIENT_ID,
                                 'redirect_uri': redirect_uri,
                                 'response_type': 'code',
                                 'layout': 'client2'})
    
    # Parse the login page
    etree = html5lib.parse(page_response.text, namespaceHTMLElements=False)
    
    # Bail if we find a request for a reCAPTCHA in the login form
    loginForm = etree.find('.//form[@name="login"]')
    if (loginForm is None) or len(loginForm.findall('.//div[@class="g-recaptcha form__recaptcha"]')) > 0:
        if loginForm is None:
            error("Could not locate login form on login page to test for reCAPTCHA, please contact the maintainer. In the meantime use a browser (Firefox recommended) to sign in at the below url and then copy & paste the full URL")
        else:
            error("gog is asking for a reCAPTCHA :(  Please use a browser (Firefox recommended) to sign in at the below url and then copy & paste the full URL")
        error(page_response.url)
        inputUrl = input("Signed In URL: ")
        try:
            parsed = urlparse(inputUrl)
            query_parsed = parse_qs(parsed.query)
            token_data['login_code'] = query_parsed['code'][0]
        except Exception:
            error("Could not parse entered URL. Try again later or report to the maintainer")
            return
    
    # Extract the login token
    for elm in etree.findall('.//input'):
        if elm.attrib.get('id') == 'login__token':
            token_data['login_token'] = elm.attrib['value']
            break
    
    if not token_data['login_code']:
        # Perform login
        page_response = request(loginSession, GOG_LOGIN_URL,
                              data={'login[username]': token_data['user'],
                                    'login[password]': token_data['passwd'],
                                    'login[login]': '',
                                    'login[_token]': token_data['login_token']})
        
        etree = html5lib.parse(page_response.text, namespaceHTMLElements=False)
        
        if 'totp' in page_response.url:
            token_data['totp_url'] = page_response.url
            for elm in etree.findall('.//input'):
                if elm.attrib.get('id') == 'two_factor_totp_authentication__token':
                    token_data['totp_token'] = elm.attrib['value']
                    break
        elif 'two_step' in page_response.url:
            token_data['two_step_url'] = page_response.url
            for elm in etree.findall('.//input'):
                if elm.attrib.get('id') == 'second_step_authentication__token':
                    token_data['two_step_token'] = elm.attrib['value']
                    break
        elif 'on_login_success' in page_response.url:
            parsed = urlparse(page_response.url)
            query_parsed = parse_qs(parsed.query)
            token_data['login_code'] = query_parsed['code'][0]
        
        # Handle TOTP authentication
        if token_data['totp_url'] is not None:
            token_data['totp_security_code'] = input("enter Authenticator security code: ")
            
            page_response = request(loginSession, token_data['totp_url'],
                                  data={'two_factor_totp_authentication[token][letter_1]': token_data['totp_security_code'][0],
                                        'two_factor_totp_authentication[token][letter_2]': token_data['totp_security_code'][1],
                                        'two_factor_totp_authentication[token][letter_3]': token_data['totp_security_code'][2],
                                        'two_factor_totp_authentication[token][letter_4]': token_data['totp_security_code'][3],
                                        'two_factor_totp_authentication[token][letter_5]': token_data['totp_security_code'][4],
                                        'two_factor_totp_authentication[token][letter_6]': token_data['totp_security_code'][5],
                                        'two_factor_totp_authentication[send]': "",
                                        'two_factor_totp_authentication[_token]': token_data['totp_token']})
            if 'on_login_success' in page_response.url:
                parsed = urlparse(page_response.url)
                query_parsed = parse_qs(parsed.query)
                token_data['login_code'] = query_parsed['code'][0]
        
        # Handle two-step authentication
        elif token_data['two_step_url'] is not None:
            token_data['two_step_security_code'] = input("enter two-step security code: ")
            
            page_response = request(loginSession, token_data['two_step_url'],
                                  data={'second_step_authentication[token][letter_1]': token_data['two_step_security_code'][0],
                                        'second_step_authentication[token][letter_2]': token_data['two_step_security_code'][1],
                                        'second_step_authentication[token][letter_3]': token_data['two_step_security_code'][2],
                                        'second_step_authentication[token][letter_4]': token_data['two_step_security_code'][3],
                                        'second_step_authentication[send]': "",
                                        'second_step_authentication[_token]': token_data['two_step_token']})
            if 'on_login_success' in page_response.url:
                parsed = urlparse(page_response.url)
                query_parsed = parse_qs(parsed.query)
                token_data['login_code'] = query_parsed['code'][0]
    
    # Exchange code for token
    if token_data['login_code']:
        token_start = time.time()
        # GOG's OAuth implementation appears to accept GET requests for token endpoint
        # Using args (not data) to match original implementation
        token_response = request(loginSession, GOG_TOKEN_URL,
                               args={'client_id': GOG_CLIENT_ID,
                                     'client_secret': GOG_SECRET,
                                     'grant_type': 'authorization_code',
                                     'code': token_data['login_code'],
                                     'redirect_uri': redirect_uri})
        token_json = token_response.json()
        token_json['expiry'] = token_start + token_json['expires_in']
        save_token(token_json, user_id=user_id)
        info('Galaxy login successful!')
    else:
        error('Galaxy login failed, verify your username/password and try again.')
        sys.exit(1)


def cmd_update(os_list, lang_list, skipknown, updateonly, partial, ids, skipids,skipHidden,installers,resumemode,strict,strictDupe,strictDownloadsUpdate,strictExtrasUpdate,md5xmls,noChangeLogs):
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


def cmd_import(src_dir, dest_dir,os_list,lang_list,skipextras,skipids,ids,skipgalaxy,skipstandalone,skipshared,destructive):
    """Recursively finds all files within root_dir and compares their MD5 values
    against known md5 values from the manifest.  If a match is found, the file will be copied
    into the game storage dir.
    """
    if destructive:
        stringOperation = "move"
        stringOperationP = "moving"
    else:
        stringOperation = "copy"
        stringOperationP = "copying"
    gamesdb = load_manifest()

    info("collecting md5 data out of the manifest")
    size_info = {} #holds dicts of entries with size as key
    #md5_info = {}  # holds tuples of (title, filename) with md5 as key

    valid_langs = []
    for lang in lang_list:
        valid_langs.append(LANG_TABLE[lang])
        
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
            _ = game.folder_name
        except AttributeError:
            game.folder_name = game.title

        downloads = game.downloads
        galaxyDownloads = game.galaxyDownloads
        sharedDownloads = game.sharedDownloads
        extras = game.extras

        if skipgalaxy:
            galaxyDownloads = []
        if skipstandalone:
            downloads = []
        if skipshared:
            sharedDownloads = []
        if skipextras:
            extras = []
                        
            
        if ids and not (game.title in ids) and not (str(game.id) in ids):
            continue
        if game.title in skipids or str(game.id) in skipids:
            continue
        for game_item in downloads+galaxyDownloads+sharedDownloads:
            if game_item.md5 is not None:
                if game_item.lang in valid_langs:
                    if game_item.os_type in os_list:
                        try:
                            md5_info = size_info[game_item.size]
                        except KeyError:
                            md5_info = {}
                        try:
                            items = md5_info[game_item.md5]
                        except Exception:
                            items = {}
                        try:
                            entry = items[(game.folder_name,game_item.name)]
                        except Exception:
                            entry = game_item
                        items[(game.folder_name,game_item.name)] = entry
                        md5_info[game_item.md5] = items
                        size_info[game_item.size] = md5_info
        #Note that Extras currently have unusual Lang / OS entries that are also accepted.  
        valid_langs_extras = valid_langs + [u'']
        valid_os_extras = os_list + [u'extra']
        for extra_item in extras:
            if extra_item.md5 is not None:
                if extra_item.lang in valid_langs_extras:
                    if extra_item.os_type in valid_os_extras:            
                        try:
                            md5_info = size_info[extra_item.size]
                        except KeyError:
                            md5_info = {}
                        try:
                            items = md5_info[extra_item.md5]
                        except Exception:
                            items = {}
                        try:
                            entry = items[(extra_item.folder_name,extra_item.name)]
                        except Exception:
                            entry = extra_item
                        items[(game.folder_name,extra_item.name)] = entry
                        md5_info[extra_item.md5] = items
                        size_info[extra_item.size] = md5_info
        
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
                        setattr(entry,"old_updated",updated)
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

def cmd_clear_partial_downloads(cleandir,dryrun):
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
