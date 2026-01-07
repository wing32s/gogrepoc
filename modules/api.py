#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import time
import datetime
import logging
import getpass
import requests
import html5lib
import xml.etree.ElementTree
import pprint
import email.utils
import threading
from urllib.parse import urlparse, unquote, urlunparse, parse_qs

from .utils import (
    AttrDict, info, warn, error, debug, log_exception,
    HTTP_TIMEOUT, HTTP_RETRY_COUNT, HTTP_RETRY_DELAY, USER_AGENT,
    TOKEN_FILENAME, SKIP_MD5_FILE_EXT,
    GOG_HOME_URL, GOG_AUTH_URL, GOG_LOGIN_URL, GOG_TOKEN_URL,
    GOG_GALAXY_REDIRECT_URL, GOG_CLIENT_ID, GOG_SECRET,
    REPO_HOME_URL, NEW_RELEASE_URL,
    append_xml_extension_to_url_path
)
from .config import get_user_paths

def input_timeout(*ignore):
    raise TimeoutError

def makeGitHubSession(authenticatedSession=False):
    gitSession = requests.Session()
    gitSession.headers={'User-Agent':USER_AGENT,'Accept':'application/vnd.github.v3+json'}
    return gitSession    
        
def makeGOGSession(loginSession=False, user_id=None):
    gogSession = requests.Session()
    if not loginSession:
        gogSession.token = load_token(user_id=user_id)
        gogSession.user_id = user_id  # Store for token renewal
        try:
            gogSession.headers={'User-Agent':USER_AGENT,'Authorization':'Bearer ' + gogSession.token['access_token']}    
        except (KeyError, AttributeError): 
            user_msg = f" for user '{user_id}'" if user_id else ""
            error(f'failed to find valid token{user_msg} (Please login and retry)')
            sys.exit(1)
    return gogSession

def save_token(token, user_id=None):
    paths = get_user_paths(user_id)
    token_path = paths['token']
    
    user_msg = f" for user '{user_id}'" if user_id else ""
    info(f'saving token{user_msg}...')
    try:
        with open(token_path, 'w', encoding='utf-8') as w:
            pprint.pprint(token, width=123, stream=w)
        info(f'saved token{user_msg}')
    except KeyboardInterrupt:
        with open(token_path, 'w', encoding='utf-8') as w:
            pprint.pprint(token, width=123, stream=w)
        info(f'saved token{user_msg}')            
        raise

def load_token(filepath=None, user_id=None):
    # Get user-specific token path
    if filepath is None:
        paths = get_user_paths(user_id)
        filepath = paths['token']
    
    user_msg = f" for user '{user_id}'" if user_id else ""
    info(f'loading token{user_msg}...')
    try:
        with open(filepath, 'r', encoding='utf-8') as r:
            ad = r.read().replace('{', 'AttrDict(**{').replace('}', '})')
        return eval(ad)
    except IOError:
        return {}

# Token renewal lock to prevent concurrent renewal attempts
token_lock = threading.RLock()

def check_and_renew_token(session, proactive_buffer=300):
    """Check token expiry and proactively renew if needed.
    
    This function prevents token expiration during long downloads by
    refreshing tokens before they expire (default: 5 minutes early).
    
    Args:
        session: GOG session object with token attribute
        proactive_buffer: Seconds before expiry to trigger renewal (default: 300 = 5 minutes)
        
    Returns:
        bool: True if token is valid (renewed if necessary), False if renewal failed
    """
    with token_lock:
        try:
            expiry = session.token.get('expiry', 0)
            time_now = time.time()
            time_until_expiry = expiry - time_now
            
            # If token expires in less than proactive_buffer seconds, renew it
            if time_until_expiry < proactive_buffer:
                if time_until_expiry < 0:
                    info(f'Token expired {abs(time_until_expiry):.0f}s ago, renewing...')
                else:
                    info(f'Token expires in {time_until_expiry:.0f}s, proactively renewing...')
                return renew_token(session)
            else:
                debug(f'Token valid for {time_until_expiry:.0f}s, no renewal needed')
            return True
        except (KeyError, AttributeError) as e:
            warn('token check failed, attempting renewal')
            return renew_token(session)

def renew_token(session, retries=HTTP_RETRY_COUNT, delay=None):
    """Renew the GOG authentication token if expired or about to expire.
    
    Args:
        session: GOG session object with token attribute
        retries: Number of retry attempts
        delay: Delay between retries
        
    Returns:
        bool: True if token was renewed successfully, False otherwise
    """
    with token_lock:
        time_now = int(time.time())
        try:
            # Refresh token if it expires within 5 minutes
            if time_now + 300 > session.token.get('expiry', 0):
                info('refreshing token')
                try:
                    token_response = session.get(
                        GOG_TOKEN_URL,
                        params={
                            'client_id': GOG_CLIENT_ID,
                            'client_secret': GOG_SECRET,
                            'grant_type': 'refresh_token',
                            'refresh_token': session.token['refresh_token']
                        },
                        timeout=HTTP_TIMEOUT
                    )
                    token_json = token_response.json()
                    
                    if token_response.status_code != 200:
                        if retries > 0:
                            warn('Token renewal failed (status %d), retrying in %ds...' % (token_response.status_code, HTTP_RETRY_DELAY))
                            time.sleep(HTTP_RETRY_DELAY)
                            return renew_token(session=session, retries=retries-1)
                        else:
                            error('Could not renew token after %d retries. Please login again.' % HTTP_RETRY_COUNT)
                            sys.exit(1)
                    
                    session.token.update(token_json)
                    session.token['expiry'] = time_now + token_json['expires_in']
                    # Get user_id from session if available
                    user_id = getattr(session, 'user_id', None)
                    save_token(session.token, user_id=user_id)
                    session.headers['Authorization'] = 'Bearer ' + session.token['access_token']
                    info('refreshed token')
                    return True
                except (requests.exceptions.RequestException, ValueError) as e:
                    if retries > 0:
                        warn('Token renewal request failed (%s), retrying in %ds...' % (e, HTTP_RETRY_DELAY))
                        time.sleep(HTTP_RETRY_DELAY)
                        return renew_token(session=session, retries=retries-1)
                    else:
                        error('Token renewal failed after %d retries: %s. Please login again.' % (HTTP_RETRY_COUNT, e))
                        sys.exit(1)
                except KeyError as e:
                    error('Token refresh failed - missing required field: %s. Please login again.' % e)
                    sys.exit(1)
        except (KeyError, AttributeError):
            error('Invalid token format. Please login again.')
            sys.exit(1)
    return False

def request(session, url, args=None, data=None, byte_range=None, stream=False):
    response = None
    retries = 0
    token_renewed = False
    while retries <= HTTP_RETRY_COUNT:
        try:
            if args:
                response = session.get(url, params=args, timeout=HTTP_TIMEOUT, stream=stream)
            elif data:
                response = session.post(url, data=data, timeout=HTTP_TIMEOUT, stream=stream)
            elif byte_range:
                headers = {'Range': 'bytes=%d-%d' % byte_range}
                response = session.get(url, headers=headers, timeout=HTTP_TIMEOUT, stream=stream)
            else:
                response = session.get(url, timeout=HTTP_TIMEOUT, stream=stream)
            response.raise_for_status()
            return response
        except (requests.HTTPError, requests.ConnectionError) as e:
            # Handle 401 Unauthorized - token expired
            if isinstance(e, requests.HTTPError) and e.response.status_code == 401:
                if not token_renewed and hasattr(session, 'token'):
                    # Use lock to prevent multiple threads from renewing simultaneously
                    with token_lock:
                        warn('401 Unauthorized - attempting to renew token')
                        if renew_token(session):
                            token_renewed = True
                            info('Token renewed, retrying request')
                            continue  # Retry with renewed token (don't increment retries)
                        else:
                            error('Token renewal failed. Please login again.')
                            sys.exit(1)
                else:
                    error('401 Unauthorized after token renewal. Please login again.')
                    sys.exit(1)
            
            if retries < HTTP_RETRY_COUNT:
                retries += 1
                if isinstance(e, requests.HTTPError) and e.response.status_code == 504:
                    debug('504 (Gateway Timeout) - waiting %ds and retrying...' % HTTP_RETRY_DELAY)
                elif isinstance(e, requests.HTTPError) and e.response.status_code == 502:
                    debug('502 (Bad Gateway) - waiting %ds and retrying...' % HTTP_RETRY_DELAY)    
                elif isinstance(e, requests.HTTPError) and e.response.status_code == 404:
                    # Don't retry 404s - they won't suddenly become available
                    raise
                else:
                    debug('%s - waiting %ds and retrying...' % (e, HTTP_RETRY_DELAY))
                time.sleep(HTTP_RETRY_DELAY)
                continue
            else: 
                if isinstance(e, requests.HTTPError):
                    # Suppress traceback for common expected errors like 404
                    if e.response.status_code in (404, 403):
                        debug('HTTP %d for %s' % (e.response.status_code, url))
                    else:
                        try:
                            error_detail = e.response.text
                            log_exception('retry count exceeded. Response: %s' % error_detail)
                        except:
                            log_exception('retry count exceeded')
                else:
                    log_exception('retry count exceeded')
                raise

def request_head(session, url, args=None, data=None):
    response = None
    retries = 0
    
    # Ensure token is valid before making request
    renew_token(session)
    
    while retries <= HTTP_RETRY_COUNT:
        try:
            if args:
                response = session.head(url, params=args, timeout=HTTP_TIMEOUT, allow_redirects=True)
            else:
                 response = session.head(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
            response.raise_for_status()
            return response
        except (requests.HTTPError, requests.ConnectionError) as e:
            if retries < HTTP_RETRY_COUNT:
                retries += 1
                if isinstance(e, requests.HTTPError) and e.response.status_code == 504:
                    debug('504 (Gateway Timeout) - waiting %ds and retrying...' % HTTP_RETRY_DELAY)
                elif isinstance(e, requests.HTTPError) and e.response.status_code == 502:
                    debug('502 (Bad Gateway) - waiting %ds and retrying...' % HTTP_RETRY_DELAY)
                elif isinstance(e, requests.HTTPError) and e.response.status_code == 404:
                    # Don't retry 404s - suppress traceback for expected missing files
                    debug('404 Not Found for %s' % url)
                    raise
                else:
                    debug('%s - waiting %ds and retrying...' % (e, HTTP_RETRY_DELAY))    
                time.sleep(HTTP_RETRY_DELAY)
                continue
            else:
                # Don't show traceback for common 404 errors
                if isinstance(e, requests.HTTPError) and e.response.status_code == 404:
                    debug('404 Not Found for %s' % url)
                else:
                    log_exception('retry count exceeded')
                raise

def fetch_chunk_tree(response, session):
    file_ext = os.path.splitext(urlparse(response.url).path)[1].lower()
    if file_ext not in SKIP_MD5_FILE_EXT:
        try:
            chunk_url = append_xml_extension_to_url_path(response.url)
            chunk_response = request(session, chunk_url)
            shelf_etree = xml.etree.ElementTree.fromstring(chunk_response.content)
            return  shelf_etree
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                debug("no md5 data found for {}".format(chunk_url))
            else:
                warn("unexpected error fetching md5 data for {} (HTTP {})".format(chunk_url, e.response.status_code))
            return None
        except xml.etree.ElementTree.ParseError:
            warn('xml parsing error occurred trying to get md5 data for {}'.format(chunk_url))
            return None
        except requests.exceptions.ConnectionError as e:
            warn("unexpected connection error fetching md5 data for {}".format(chunk_url) + " This error may be temporary. Please retry in 24 hours.")
            return None 
        except requests.exceptions.ContentDecodingError as e:
            warn("unexpected content decoding error fetching md5 data for {}".format(chunk_url) + " This error may be temporary. Please retry in 24 hours.")
            debug("The handled exception was:")
            log_exception('')                
            debug("End exception report.")
            return None 
    return None

def fetch_file_info(d, fetch_md5, save_md5_xml, updateSession):
   # fetch file name/size
    #try:
    response= request_head(updateSession, d.href)
    #except ContentDecodingError as e:
        #info('decoding failed because getting 0 bytes')
        #response = e.response

    d.gog_data.headers = AttrDict()
    d.gog_data.original_headers = AttrDict()
    for key in response.headers.keys():
        d.gog_data.original_headers[key] = response.headers[key]
    for key in d.gog_data.original_headers:
        d.gog_data.headers[key.lower()] = d.gog_data.original_headers[key]
    
    # Validate that GOG didn't return an error page (HTML) instead of the actual file
    content_type = d.gog_data.headers.get('content-type', '').lower()
    content_length = int(d.gog_data.headers.get('content-length', 0))
    
    if 'text/html' in content_type:
        error(f"GOG returned HTML error page instead of file for {d.href}")
        error(f"Content-Type: {content_type}, Content-Length: {content_length}")
        d.name = None  # Signal to skip this file
        d.size = None
        return
    
    # Warn if file size seems suspiciously small (likely an error)
    if content_length > 0 and content_length < 5000:
        warn(f"Suspiciously small file size ({content_length} bytes) for {d.href} - may be an error response")
    
    # Try to get filename from Content-Disposition header first
    d.name = None
    if 'content-disposition' in d.gog_data.headers:
        content_disp = d.gog_data.headers['content-disposition']
        # Parse Content-Disposition: attachment; filename="setup_game.exe"
        if 'filename=' in content_disp:
            filename_part = content_disp.split('filename=')[1]
            # Remove quotes if present
            d.name = filename_part.strip('"').strip("'")
    
    # Fallback to URL path if Content-Disposition not available
    if not d.name:
        d.name = unquote(urlparse(response.url).path.split('/')[-1])
    
    # Debug log for numeric-only filenames (common for extras)
    if d.name and d.name.isdigit():
        debug(f"Numeric filename '{d.name}' from {d.href} (common for extras)")
    
    d.size = content_length

    # fetch file md5
    if fetch_md5:
        file_ext = os.path.splitext(urlparse(response.url).path)[1].lower()
        if file_ext not in SKIP_MD5_FILE_EXT:
            try:
                tmp_md5_url = append_xml_extension_to_url_path(response.url)
                md5_response = request(updateSession, tmp_md5_url)
                shelf_etree = xml.etree.ElementTree.fromstring(md5_response.content)
                d.gog_data.md5_xml = AttrDict()
                d.gog_data.md5_xml.tag = shelf_etree.tag
                for key in shelf_etree.attrib.keys():
                    d.gog_data.md5_xml[key] = shelf_etree.attrib.get(key)
                if (save_md5_xml):    
                    d.gog_data.md5_xml.text = md5_response.text
                d.md5 = shelf_etree.attrib['md5']
                d.raw_updated = shelf_etree.attrib['timestamp']
                d.updated = datetime.datetime.fromisoformat(d.raw_updated).replace(tzinfo=datetime.timezone.utc).isoformat()
            except requests.HTTPError as e:
                if e.response.status_code == 404:
                    debug("no md5 data found for {}".format(d.name))
                else:
                    warn("unexpected error fetching md5 data for {} (HTTP {})".format(d.name, e.response.status_code))
            except xml.etree.ElementTree.ParseError as e:
                warn('xml parsing error occurred trying to get md5 data for {}'.format(d.name))
            except requests.exceptions.ConnectionError as e:
                warn("unexpected connection error fetching md5 data for {}".format(d.name) + " This error may be temporary. Please retry in 24 hours.")
            except requests.exceptions.ContentDecodingError as e:
                warn("unexpected content decoding error fetching md5 data for {}".format(d.name) + " This error may be temporary. Please retry in 24 hours.")
                debug("The handled exception was:")
                log_exception('')                
                debug("End exception report.")
        else:
            d.md5_exempt = True
    if d.updated == None:
        # Safely get the last-modified header, fall back to a default if not present
        last_modified = d.gog_data.headers.get("last-modified")
        if last_modified:
            d.raw_updated = last_modified
            d.updated = email.utils.parsedate_to_datetime(d.raw_updated).isoformat() #Standardize
        else:
            # If no last-modified header, use current time as fallback
            import datetime
            d.raw_updated = email.utils.formatdate(usegmt=True)
            d.updated = datetime.datetime.now().isoformat()
