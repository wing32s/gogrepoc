#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import logging
import zipfile
import hashlib
import re
import platform
import ctypes
import threading
import contextlib
import time
import shutil
import unicodedata

# Optional imports
try:
    from html2text import html2text
except ImportError:
    def html2text(x): return x

# Basic constants
__appname__ = 'gogrepoc'
__version__ = '0.3.4a-Gamma'
__author__  = 'Kalanyr'
__licence__ = 'GPLv3'
__url__     = 'http://github.com/Kalanyr/gogrepoc'

# Logging constants
LOG_FILENAME = 'gogrepo.log'

# HTTP constants
HTTP_TIMEOUT = 300
HTTP_RETRY_COUNT = 3
HTTP_RETRY_DELAY = 3        # seconds
HTTP_GAME_DOWNLOADER_THREADS = 4
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36'

# GOG API Constants
GOG_HOME_URL = 'https://www.gog.com'
GOG_ACCOUNT_URL = 'https://www.gog.com/account'
GOG_AUTH_URL = 'https://login.gog.com/auth'
GOG_LOGIN_URL = 'https://login.gog.com/login_check'
GOG_TOKEN_URL = 'https://auth.gog.com/token'
GOG_GALAXY_REDIRECT_URL = 'https://embed.gog.com/on_login_success'
GOG_CLIENT_ID = '46899977096215655'
GOG_SECRET = '9d85c43b1482497dbbce61f6e4aa173a433796eeae2ca8c5f6129f2dc4de46d9'
GOG_MEDIA_TYPE_GAME = '1'
GOG_MEDIA_TYPE_MOVIE = '2'

# GitHub Constants
REPO_HOME_URL = 'https://api.github.com/repos/Kalanyr/gogrepoc'
NEW_RELEASE_URL = '/releases/latest'

# File and Directory Constants
MANIFEST_FILENAME = 'gog-manifest.dat'
RESUME_MANIFEST_FILENAME = 'gog-resume-manifest.dat'
CONFIG_FILENAME = 'gogrepo.config'
TOKEN_FILENAME = 'gog-token.dat'
MD5_DIR_NAME = '!md5'
MD5_DB = 'gog-md5.db'
DOWNLOADING_DIR_NAME = '!downloading'
PROVISIONAL_DIR_NAME = '!provisional'
ORPHAN_DIR_NAME = '!orphaned'
IMAGES_DIR_NAME = '!images'
INFO_FILENAME = '!info.txt'
SERIAL_FILENAME = '!serial.txt'
GAME_STORAGE_DIR = 'downloads'

RESUME_MANIFEST_SYNTAX_VERSION = 1
RESUME_SAVE_THRESHOLD = 50

# Lists
VALID_OS_TYPES = ['windows', 'mac', 'linux']

# LANG_TABLE maps specific language codes to those used in file naming and API
LANG_TABLE = {
    'en': 'English', 'bl': 'български', 'ru': 'русский', 'gk': 'Ελληνικά',
    'sb': 'Српска', 'ar': 'العربية', 'br': 'Português do Brasil', 'jp': '日本語',
    'ko': '한국어', 'fr': 'français', 'cn': '中文', 'cz': 'český',
    'hu': 'magyar', 'pt': 'português', 'tr': 'Türkçe', 'sk': 'slovenský',
    'nl': 'nederlands', 'ro': 'română', 'es': 'español', 'pl': 'polski',
    'it': 'italiano', 'de': 'Deutsch', 'da': 'Dansk', 'sv': 'svenska',
    'fi': 'Suomi', 'no': 'norsk', 'th': 'ไทย'
}

VALID_LANG_TYPES = list(LANG_TABLE.keys())

SKIP_MD5_FILE_EXT = ['.zip', '.exe', '.bin', '.dmg', '.sh', '.pkg', '.deb', '.tar.gz', '.pkg.tar.xz', '.rar', '.mp4']
INSTALLERS_EXT = ['.exe', '.bin', '.dmg', '.pkg', '.sh']
ORPHAN_DIR_EXCLUDE_LIST = ['!downloads'.lower(), '!downloading'.lower(), '!orphaned'.lower(), '!terraform'.lower(), '!md5'.lower()]
ORPHAN_FILE_EXCLUDE_LIST = ['gogrepo.py', 'gogrepoc.py', 'gogrepo.config', 'pylru.py', 'pylru.pyc', 'gogrepo.log',
                            'html2text.py', 'html2text.pyc', 'manifest.json', 'manifest.resume', 'token', 'token.json']

WINDOWS_PREALLOCATION_FS = ["NTFS"]
POSIX_PREALLOCATION_FS = ["ext4", "btrfs", "xfs", "ocfs2", "gfs2", "tmpfs"]

# Long path handling for Windows
if platform.system() == "Windows":
    uLongPathPrefix = u"\\\\?\\"
    import ctypes.wintypes
    GENERIC_READ = 0x80000000
    GENERIC_WRITE = 0x40000000
    FILE_BEGIN = 0
    OPEN_EXISTING = 3
    CREATE_NEW = 1
else:
    # Non-Windows systems don't need long path prefix
    uLongPathPrefix = u""
    # Dummy values for non-Windows systems (these constants won't be used)
    GENERIC_READ = 0
    GENERIC_WRITE = 0
    FILE_BEGIN = 0
    OPEN_EXISTING = 0
    CREATE_NEW = 0

# Setup Logging
rootLogger = logging.getLogger('')
loggingHandler = logging.FileHandler(LOG_FILENAME, 'w', 'utf-8')
loggingHandler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s'))
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logging.Formatter('%(message)s'))
rootLogger.addHandler(loggingHandler)  # Add file handler to actually log to file
rootLogger.addHandler(consoleHandler)
rootLogger.setLevel(logging.INFO)

def log_exception(msg):
    rootLogger.error(msg, exc_info=True)

def info(msg):
    rootLogger.info(msg)

def warn(msg):
    rootLogger.warning(msg)

def error(msg):
    rootLogger.error(msg)

def debug(msg):
    rootLogger.debug(msg)

class AttrDict(dict):
    """A dictionary that can be accessed with dot notation."""
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

class ConditionalWriter:
    """A context manager that writes to a file only if the new content is different from existing content."""
    def __init__(self, path):
        self.path = path
        self.io = None
        self.content = None

    def __enter__(self):
        from io import StringIO
        self.io = StringIO()
        return self.io

    def __exit__(self, type, value, traceback):
        self.content = self.io.getvalue()
        self.io.close()
        write = True
        try:
           if os.path.exists(self.path):
               with open(self.path, "r", encoding="utf-8") as f:
                   old_content = f.read()
                   if old_content == self.content:
                       write = False
        except Exception:
            pass # If checking fails, writing is safer
            
        if write:
             with open(self.path, "w", encoding="utf-8") as f:
                   f.write(self.content)

class open_notrunc:
    """
    Opens a file for r+b but does not truncate it.
    Useful for pre-allocation or partial downloads.
    """
    def __init__(self, path):
        self.path = path
        self.f = None
        
    def __enter__(self):
        if not os.path.exists(self.path):
            self.f = open(self.path, "wb")
        else:    
            self.f = open(self.path, "r+b")
        return self.f
        
    def __exit__(self, type, value, traceback):
        self.f.close() 

class open_notruncwrrd:
    """
    Opens a file for reading/writing but does not truncate it. 
    It is specifically for 'rb+' modes where you might want to switch between reading and writing.
    """
    def __init__(self, path):
        self.path = path
        self.f = None
        
    def __enter__(self):
        if not os.path.exists(self.path):
            self.f = open(self.path, "w+b")
        else:    
            self.f = open(self.path, "r+b")
        return self.f
        
    def __exit__(self, type, value, traceback):
        self.f.close() 

def hashfile(file):
    """Calculates MD5 hash of a file."""
    BLOCKSIZE = 65536
    hasher = hashlib.md5()
    with open(file, 'rb') as afile:
        buf = afile.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(BLOCKSIZE)
    return hasher.hexdigest()

def hashstream(stream, start, end):
    """Calculates MD5 hash of a stream segment."""
    BLOCKSIZE = 65536
    hasher = hashlib.md5()
    
    stream.seek(start)
    
    # Check bounds
    try:
        if stream.tell() != start:
            raise IOError("Could not seek to start of stream")
    except ValueError:
         pass # Ignore specific seek errors if tell works or logic differs
         
    sz = (end - start) + 1
    
    buf = stream.read(min(BLOCKSIZE, sz))
    while len(buf) > 0:
        hasher.update(buf)
        sz -= len(buf)
        if sz <= 0:
            break
        buf = stream.read(min(BLOCKSIZE, sz))
    return hasher.hexdigest()

def check_skip_file(fname, skipfiles):
    """Checks if a filename matches any of the skip patterns."""
    from fnmatch import fnmatch
    for skipf in skipfiles:
        if fnmatch(fname, skipf):
            return skipf
    return None

def process_path(path):
    """Standardizes path format and handles long paths on Windows."""
    fpath = path
    fpath = os.path.abspath(fpath)
    if platform.system() == "Windows":
        raw_fpath = u'\\\\?\\%s' % fpath
        return raw_fpath
    return fpath

def is_numeric_id(s):
    try:
        int(s)
        return True
    except ValueError:
        return False    

def append_xml_extension_to_url_path(url):
    from urllib.parse import urlparse, urlunparse
    parsed = urlparse(url)
    return urlunparse(parsed._replace(path = parsed.path + ".xml")).replace('%28','(').replace('%29',')') 

def pretty_size(b):
    """Returns a purely human readable size string."""
    if b < 1024:
        return '%iB' % b
    elif b < 1024 * 1024:
        return '%.2fKB' % (b / 1024.0)
    elif b < 1024 * 1024 * 1024:
        return '%.2fMB' % (b / 1024.0 / 1024.0)
    else:
        return '%.2fGB' % (b / 1024.0 / 1024.0 / 1024.0)

def get_total_size(path):
    """Calculates total size of a directory recursively."""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                total_size += os.path.getsize(fp)
            except OSError:
                pass # Ignore if file permission error or file removed
    return total_size

def get_fs_type(path, windows_magic=False):
    """
    Implementation to guess filesystem type.
    Note: 'windows_magic' parameter was in loose usage in original script.
    """
    root_type = ""
    # Simplified logic to determine filesystem type or root
    best_match = ""
    if platform.system() == "Windows":
        return "NTFS" # Default assumption for Windows usually, or use external call
        # In original script this was more complex or specific. For now, defaulting.
        # Original code didn't have a full implementation visible in main chunks for Windows FS detection 
        # other than calls to `fsutil` inside comments or similar. 
        # Actually wait, original script had logic for linux `df -T`.
    
    if platform.system() != "Windows":
         try:
             import subprocess
             p = subprocess.Popen(['df', '-T', path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
             out, err = p.communicate()
             # usually second line, second column
             lines = out.strip().splitlines()
             if len(lines) >= 2:
                 parts = lines[1].split()
                 if len(parts) >= 2:
                     root_type = parts[1]
         except Exception:
             pass
    return root_type

def test_zipfile(filepath):
    """Tests integrity of a zip file."""
    try:
        with zipfile.ZipFile(filepath) as z:
            ret = z.testzip()
            if ret is not None:
                return False
        return True
    except (zipfile.BadZipFile, zipfile.LargeZipFile):
         return False
    except NotImplementedError:
        raise # Compression not supported

def move_with_increment_on_clash(src, dest):
    """
    Moves a file or directory. If destination exists, appends incrementing number.
    """
    base, ext = os.path.splitext(dest)
    i = 1
    target = dest
    while os.path.exists(target):
         target = "{}_{}{}".format(base, i, ext)
         i += 1
    shutil.move(src, target)

def slugify(value, allow_unicode=False):
    """
    Django-like slugify.
    Convert to ASCII if 'allow_unicode' is False. Convert spaces to hyphens.
    Remove characters that aren't alphanumerics, underscores, or hyphens.
    Convert to lowercase.
    """
    import unicodedata
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value.lower())
    return re.sub(r'[-\s]+', '-', value).strip('-_')


# --- Classes to prevent computer going to sleep during large downloads ---

class DBusSystemInhibitor:
    def __init__(self, name, path, interface, method=["Inhibit"]):
        try:
            import PyQt5.QtDBus
            from xml.etree import ElementTree
        except ImportError:
            raise
            
        self.name = name
        self.path = path
        self.interface_name = interface
        self.method = method
        self.cookie = None
        self.APPNAME = "GOGRepo Gamma"
        self.REASON = "Using Internet and USB Connection"
        bus = PyQt5.QtDBus.QDBusConnection.systemBus()
        introspection = PyQt5.QtDBus.QDBusInterface(self.name, self.path, "org.freedesktop.DBus.Introspectable", bus) 
        serviceIntrospection = ElementTree.fromstring(PyQt5.QtDBus.QDBusReply(introspection.call("Introspect")).value())
        methodExists = False                                           
        for interface in serviceIntrospection.iter("interface"):
            if interface.get('name') == self.interface_name:      
                for method in interface.iter("method"):
                    if method.get('name') == self.method[0]:
                        methodExists = True
        if not methodExists:
            raise AttributeError(self.interface_name + "has no method " + self.method[0])
        self.iface = PyQt5.QtDBus.QDBusInterface(self.name, self.path, self.interface_name, bus)   
        
    def inhibit(self):
        import PyQt5.QtDBus
        if self.cookie is None:
            reply = PyQt5.QtDBus.QDBusReply(self.iface.call(self.method[0], "idle", self.APPNAME, self.REASON, "block"))
            if reply.isValid():
                self.cookie = reply.value()
        
    def uninhibit(self):
        if (self.cookie is not None):
            pass 

class DBusSessionInhibitor:
    def __init__(self, name, path, interface, methods=["Inhibit", "UnInhibit"]):
        try:
            import PyQt5.QtDBus
        except ImportError:
            raise
            
        self.name = name
        self.path = path
        self.interface_name = interface
        self.methods = methods
        self.cookie = None
        self.APPNAME = "GOGRepo Gamma"
        self.REASON = "Using Internet and USB Connection"

        bus = PyQt5.QtDBus.QDBusConnection.sessionBus()
        self.iface = PyQt5.QtDBus.QDBusInterface(self.name, self.path, self.interface_name, bus)   

    def inhibit(self):
        import PyQt5.QtDBus
        if self.cookie is None:
            self.cookie = PyQt5.QtDBus.QDBusReply(self.iface.call(self.methods[0], self.APPNAME, self.REASON)).value()

    def uninhibit(self):
        if self.cookie is not None:
            self.iface.call(self.methods[1], self.cookie)
            self.cookie = None

class GnomeSessionInhibitor(DBusSessionInhibitor):
    TOPLEVEL_XID = 0
    INHIBIT_SUSPEND = 4

    def __init__(self):
        DBusSessionInhibitor.__init__(self, 'org.gnome.SessionManager',
                                '/org/gnome/SessionManager',
                                "org.gnome.SessionManager",
                                ["Inhibit", "Uninhibit"])

    def inhibit(self):
        import PyQt5.QtDBus
        if self.cookie is None:
            self.cookie = PyQt5.QtDBus.QDBusReply(self.iface.call(self.methods[0], self.APPNAME, GnomeSessionInhibitor.TOPLEVEL_XID, self.REASON), GnomeSessionInhibitor.INHIBIT_SUSPEND).value()

class Wakelock: 
    # Mac Sleep support based on caffeine : https://github.com/jpn--/caffeine by Jeffrey Newman

    def __init__(self):
        if (platform.system() == "Windows"):
            self.ES_CONTINUOUS        = 0x80000000
            self.ES_AWAYMODE_REQUIRED = 0x00000040
            self.ES_SYSTEM_REQUIRED   = 0x00000001
            self.ES_DISPLAY_REQUIRED  = 0x00000002
            self.ES_WAKELOCK = self.ES_CONTINUOUS | self.ES_SYSTEM_REQUIRED | self.ES_DISPLAY_REQUIRED
            
        if (platform.system() == "Darwin"):
            try:
                import objc
                import CoreFoundation
            except ImportError:
                pass # Should prob log warning but dependencies might be missing
                
            self.PM_NODISPLAYSLEEP = 'NoDisplaySleepAssertion'
            self.PM_NOIDLESLEEP = "NoIdleSleepAssertion"
            self.PM_WAKELOCK = self.PM_NOIDLESLEEP
            self._kIOPMAssertionLevelOn = 255
            
            self.libIOKit = ctypes.cdll.LoadLibrary('/System/Library/Frameworks/IOKit.framework/IOKit')
            self.libIOKit.IOPMAssertionCreateWithName.argtypes = [ ctypes.c_void_p, ctypes.c_uint32, ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32) ]
            self.libIOKit.IOPMAssertionRelease.argtypes = [ ctypes.c_uint32 ]
            self._PMassertion = None 
            self._PMassertID = ctypes.c_uint32(0) 
            self._PMerrcode = None
            self._IOPMAssertionRelease = self.libIOKit.IOPMAssertionRelease

    def _CFSTR(self, py_string):
        import CoreFoundation
        return CoreFoundation.CFStringCreateWithCString(None, py_string.encode('utf-8'), CoreFoundation.kCFStringEncodingUTF8)

    def raw_ptr(self, pyobjc_string):
        import objc
        return objc.pyobjc_id(pyobjc_string.nsstring())

    def _IOPMAssertionCreateWithName(self, assert_name, assert_level, assert_msg):
        assertID = ctypes.c_uint32(0)
        p_assert_name = self.raw_ptr(self._CFSTR(assert_name))
        p_assert_msg = self.raw_ptr(self._CFSTR(assert_msg))
        errcode = self.libIOKit.IOPMAssertionCreateWithName(p_assert_name,
            assert_level, p_assert_msg, ctypes.byref(assertID))
        return (errcode, assertID)
                    
    def _get_inhibitor(self):
        try:
            return DBusSystemInhibitor('org.freedesktop.login1', '/org/freedesktop/login1', 'org.freedesktop.login1.Manager')
        except Exception as e:
            warn("Could not initialise the systemd session inhibitor: %s" % e)
        return None
    
    def take_wakelock(self):    
        if platform.system() == "Windows":
            ctypes.windll.kernel32.SetThreadExecutionState(self.ES_WAKELOCK)
        if platform.system() == "Darwin":
            try:
                 a = self.PM_WAKELOCK
                 if self._PMassertion is not None and a != self._PMassertion:
                     self.release_wakelock()
                 if self._PMassertID.value == 0:
                     self._PMerrcode, self._PMassertID = self._IOPMAssertionCreateWithName(a, self._kIOPMAssertionLevelOn, "gogrepoc")
                     self._PMassertion = a
            except Exception:
                pass
        if (not (platform.system() == "Windows" or platform.system() == "Darwin")) and ('PyQt5.QtDBus' in sys.modules):
            self.inhibitor = self._get_inhibitor()
            if (self.inhibitor != None):
                self.inhibitor.inhibit()
        
    def release_wakelock(self):
        if platform.system() == "Windows":
            ctypes.windll.kernel32.SetThreadExecutionState(self.ES_CONTINUOUS)
        if platform.system() == "Darwin":
            try:
                self._PMerrcode = self._IOPMAssertionRelease(self._PMassertID)
                self._PMassertID.value = 0
                self._PMassertion = None
            except Exception:
                pass


def build_md5_lookup(gamesdb, game_filter):
    """Build a lookup dictionary for MD5 matching during import operations.
    
    Creates a nested dictionary structure: size -> md5 -> (folder_name, filename) -> game_item
    This allows fast lookups when matching files by size and MD5 hash.
    
    Args:
        gamesdb: List of game items from the manifest
        game_filter: GameFilter object specifying which games/content to include
        
    Returns:
        dict: Nested dictionary mapping file size to MD5 to game items
        
    Example structure:
        {
            1024: {  # file size in bytes
                'abc123...': {  # MD5 hash
                    ('game_folder', 'file.exe'): <game_item_object>
                }
            }
        }
    """
    size_info = {}
    
    valid_langs = []
    for lang in game_filter.lang_list:
        valid_langs.append(LANG_TABLE[lang])
    
    for game in gamesdb:
        # Ensure required attributes exist
        try:
            _ = game.galaxyDownloads
        except AttributeError:
            game.galaxyDownloads = []
            
        try:
            _ = game.sharedDownloads
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

        # Apply installer type filtering
        if game_filter.installers == 'standalone':
            galaxyDownloads = []
            sharedDownloads = []
        elif game_filter.installers == 'galaxy':
            downloads = []
            sharedDownloads = []
        elif game_filter.installers == 'shared':
            downloads = []
            galaxyDownloads = []
        
        if game_filter.skip_extras:
            extras = []
        
        # Import should_process_game_by_id here to avoid circular imports
        from .game_filter import should_process_game_by_id
        if not should_process_game_by_id(game, game_filter):
            continue
            
        # Process downloads (installers)
        for game_item in downloads + galaxyDownloads + sharedDownloads:
            if game_item.md5 is not None:
                if game_item.lang in valid_langs:
                    if game_item.os_type in game_filter.os_list:
                        _add_to_md5_lookup(size_info, game_item, game.folder_name)
        
        # Process extras (note: extras have more lenient lang/os requirements)
        valid_langs_extras = valid_langs + [u'']
        valid_os_extras = game_filter.os_list + [u'extra']
        for extra_item in extras:
            if extra_item.md5 is not None:
                if extra_item.lang in valid_langs_extras:
                    if extra_item.os_type in valid_os_extras:
                        _add_to_md5_lookup(size_info, extra_item, game.folder_name)
    
    return size_info


def _add_to_md5_lookup(size_info, item, folder_name):
    """Helper function to add an item to the MD5 lookup dictionary.
    
    Args:
        size_info: The size->md5 lookup dictionary to add to
        item: The game item (download or extra) to add
        folder_name: The folder name for this game
    """
    try:
        md5_info = size_info[item.size]
    except KeyError:
        md5_info = {}
    
    try:
        items = md5_info[item.md5]
    except Exception:
        items = {}
    
    try:
        entry = items[(folder_name, item.name)]
    except Exception:
        entry = item
    
    items[(folder_name, item.name)] = entry
    md5_info[item.md5] = items
    size_info[item.size] = md5_info
