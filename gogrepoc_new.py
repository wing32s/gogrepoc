# The following code block between #START# and #END#
# generates an error message if this script is called as a shell script.
# Using a "shebang" instead would fail on Windows.
#START#
if False:
    print("Please start this script with a python interpreter: python /path/to/gogrepoc.py")
#END#
__appname__ = 'gogrepoc.py'
__author__ = 'eddie3,kalaynr'
__version__ = '0.4.0-a'
__url__ = 'https://github.com/kalanyr/gogrepoc'

# Standard library imports
import sys
import os
import time
import datetime
import argparse
import platform
import locale
import logging
import logging.handlers

# Module imports - all modularized functions
from modules.utils import (
    info, warn, error, log_exception,
    Wakelock,
    VALID_OS_TYPES, VALID_LANG_TYPES
)
from modules.config import validate_user_id
from modules.commands import (
    cmd_login, cmd_update, cmd_import,
    cmd_backup, cmd_verify, cmd_clean,
    cmd_trash, cmd_clear_partial_downloads
)
from modules.download import cmd_download

minPy3 = [3,8]

if sys.version_info[0] < 3:
    print("Your Python version is not supported, please update to 3.8+")
    sys.exit(1)
elif sys.version_info[0] == 3 and (sys.version_info[1] < minPy3[1]):
    print("Your Python version is not supported, please update to 3.8+")
    sys.exit(1)

# Configure logging
LOG_MAX_MB = 180
LOG_BACKUPS = 9 
logFormatter = logging.Formatter("%(asctime)s | %(message)s", datefmt='%H:%M:%S')
rootLogger = logging.getLogger('ws')
rootLogger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler(sys.stdout)
loggingHandler = logging.handlers.RotatingFileHandler('gogrepo.log', mode='a+', maxBytes = 1024*1024*LOG_MAX_MB , backupCount = LOG_BACKUPS,  encoding=None, delay=True)
loggingHandler.setFormatter(logFormatter)
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

# Constants
GAME_STORAGE_DIR = r'games'  # Default directory for downloaded games
MD5_DIR_NAME = '!md5_xmls'
storeExtend = 'extend'  # argparse action for extending list arguments

# Calculate system defaults
DEFAULT_FALLBACK_LANG = 'en'

sysOS = platform.system() 
sysOS = sysOS.lower()    
if sysOS == 'darwin':
    sysOS = 'mac'
if sysOS == "java":
    print("Jython is not currently supported. Let me know if you want Jython support.")
    sys.exit(1)
if not (sysOS in VALID_OS_TYPES):
    sysOS = 'linux'
DEFAULT_OS_LIST = [sysOS]

sysLang,_ = locale.getlocale()
if (sysLang is not None):
    sysLang = sysLang[:2]
    sysLang = sysLang.lower()
if not (sysLang in VALID_LANG_TYPES):
    sysLang = 'en'
DEFAULT_LANG_LIST = [sysLang]

# Helper functions for common argument patterns
def add_common_flags(parser):
    """Add common -nolog and -debug flags to a parser"""
    parser.add_argument('-nolog', action='store_true', help='doesn\'t write log file gogrepo.log')
    parser.add_argument('-debug', action='store_true', help='Includes debug messages')

def add_id_filters(parser):
    """Add mutually exclusive -ids and -skipids arguments"""
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-ids', action='store', help='id(s) or title(s) of game(s) in manifest', nargs='*', default=[])
    group.add_argument('-skipids', action='store', help='id(s) or title(s) of game(s) to skip', nargs='*', default=[])
    return group

def add_os_filters(parser, action='store'):
    """Add mutually exclusive -os and -skipos arguments"""
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-skipos', action='store', help='skip files for operating system(s)', nargs='*', default=[])
    group.add_argument('-os', action=action, help='files only for operating system(s)', nargs='*', default=[])
    return group

def add_lang_filters(parser, action='store'):
    """Add mutually exclusive -lang and -skiplang arguments"""
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-lang', action=action, help='files only for language(s)', nargs='*', default=[])
    group.add_argument('-skiplang', action=action, help='skip files for language(s)', nargs='*', default=[])
    return group

def add_installer_type_flags(parser):
    """Add installer type filter flags"""
    parser.add_argument('-skipgalaxy', action='store_true', help='skip GOG Galaxy installer files')
    parser.add_argument('-skipstandalone', action='store_true', help='skip GOG standalone installer files')
    parser.add_argument('-skipshared', action='store_true', help='skip installers shared between Galaxy and standalone')

def add_login_command(subparsers):
    """Add login command arguments"""
    parser = subparsers.add_parser(
        'login',
        help='Login to GOG and save authenticated token',
        description='Authenticate with GOG and save your access token locally. '
                    'Required before using update or download commands. '
                    'Token expires after ~1 hour but will auto-refresh.'
    )
    parser.add_argument('username', action='store', help='GOG username/email (will prompt if not provided)', nargs='?', default=None)
    parser.add_argument('password', action='store', help='GOG password (will prompt if not provided)', nargs='?', default=None)
    add_common_flags(parser)

def add_update_command(subparsers):
    """Add update command arguments"""
    parser = subparsers.add_parser(
        'update',
        help='Update locally saved game manifest from GOG server',
        description='Fetch game information from GOG and update local manifest. '
                    'Run this before downloading to get latest game versions. '
                    'Use -full to scan entire library, or -ids for specific games.'
    )
    parser.add_argument('-resumemode', action='store', choices=['noresume', 'resume', 'onlyresume'], default='resume', help='how to handle resuming if necessary')
    parser.add_argument('-strictverify', action='store_true', help='clear previously verified unless md5 match')
    parser.add_argument('-strictdupe', action='store_true', help='missing MD5s do not default to checking only file size')
    parser.add_argument('-lenientdownloadsupdate', action='store_false', help='Does not mark installers for updating if last updated time changed')
    parser.add_argument('-strictextrasupdate', action='store_true', help='Marks extras for updating if last updated time changed')
    parser.add_argument('-md5xmls', action='store_true', help='Downloads MD5 XML files to ' + MD5_DIR_NAME)
    parser.add_argument('-nochangelogs', action='store_true', help='Skips saving the changelogs for games')
    add_os_filters(parser, action=storeExtend)
    add_lang_filters(parser, action=storeExtend)
    parser.add_argument('-skiphidden', action='store_true', help='skip games marked as hidden')
    parser.add_argument('-installers', action='store', choices=['standalone', 'both'], default='standalone', help='GOG Installer type (Deprecated)')
    g4 = parser.add_mutually_exclusive_group()
    g4.add_argument('-standard', action='store_true', help='new and updated games only (default)')
    g4.add_argument('-skipknown', action='store_true', help='skip games already known by manifest')
    g4.add_argument('-updateonly', action='store_true', help='only games marked with update tag')
    g4.add_argument('-full', action='store_true', help='all games on your account')
    add_id_filters(parser)
    parser.add_argument('-wait', action='store', type=float, help='wait this long in hours before starting', default=0.0)
    add_common_flags(parser)

def add_download_command(subparsers):
    """Add download command arguments"""
    parser = subparsers.add_parser(
        'download',
        help='Download all your GOG games and extra files',
        description='Download games from your GOG library. '
                    'By default downloads all games for all OS/languages. '
                    'Use -ids to download specific games, -os/-lang to filter by platform/language.'
    )
    parser.add_argument('savedir', action='store', help='directory to save downloads to (default: games)', nargs='?', default=GAME_STORAGE_DIR)
    parser.add_argument('-dryrun', action='store_true', help='show what would be downloaded without downloading')
    add_installer_type_flags(parser)
    g2 = parser.add_mutually_exclusive_group()
    g2.add_argument('-skipextras', action='store_true', help='skip downloading of any GOG extra files')
    g2.add_argument('-skipgames', action='store_true', help='skip downloading of any GOG game files (deprecated)')
    g3 = add_id_filters(parser)
    g3.add_argument('-id', action='store', help='(deprecated) id or title of game to download')
    parser.add_argument('-covers', action='store_true', help='downloads cover images for each game')
    parser.add_argument('-backgrounds', action='store_true', help='downloads background images for each game')
    parser.add_argument('-nocleanimages', action='store_true', help='delete rather than clean old images')
    parser.add_argument('-skipfiles', action='store', help='file name (or glob patterns) to NOT download', nargs='*', default=[])
    parser.add_argument('-wait', action='store', type=float, help='wait this long in hours before starting', default=0.0)
    parser.add_argument('-downloadlimit', action='store', type=float, help='limit downloads to this many MB', default=None)
    add_os_filters(parser, action=storeExtend)
    add_lang_filters(parser, action=storeExtend)
    parser.add_argument('-skippreallocation', action='store_true', help='do not preallocate space for files')
    add_common_flags(parser)

def add_import_command(subparsers):
    """Add import command arguments"""
    parser = subparsers.add_parser('import', help='Import files with matching MD5 checksums from manifest')
    parser.add_argument('src_dir', action='store', help='source directory to import games from')
    parser.add_argument('dest_dir', action='store', help='directory to copy and name imported files to')
    add_os_filters(parser)
    add_lang_filters(parser)
    add_installer_type_flags(parser)
    add_id_filters(parser)
    add_common_flags(parser)

def add_backup_command(subparsers):
    """Add backup command arguments"""
    parser = subparsers.add_parser('backup', help='Perform an incremental backup to specified directory')
    parser.add_argument('src_dir', action='store', help='source directory containing gog items')
    parser.add_argument('dest_dir', action='store', help='destination directory to backup files to')
    add_id_filters(parser)
    add_os_filters(parser)
    add_lang_filters(parser)
    g4 = parser.add_mutually_exclusive_group()
    g4.add_argument('-skipextras', action='store_true', help='skip backup of any GOG extra files')
    g4.add_argument('-skipgames', action='store_true', help='skip backup of any GOG game files')
    add_installer_type_flags(parser)
    add_common_flags(parser)

def add_verify_command(subparsers):
    """Add verify command arguments"""
    parser = subparsers.add_parser(
        'verify',
        help='Scan and verify downloaded GOG files (size, MD5, zip integrity)',
        description='Verify integrity of downloaded files by checking file size, MD5 checksums, '
                    'and zip file integrity. Failed files can be deleted (-delete) or cleaned (-clean).'
    )
    parser.add_argument('gamedir', action='store', help='directory containing games to verify', nargs='?', default=GAME_STORAGE_DIR)
    parser.add_argument('-permissivechangeclear', action='store_true', help='clear change marking for files that pass test')
    parser.add_argument('-forceverify', action='store_true', help='verify files unchanged since last verification')
    parser.add_argument('-skipmd5', action='store_true', help='do not perform MD5 check')
    parser.add_argument('-skipsize', action='store_true', help='do not perform size check')
    parser.add_argument('-skipzip', action='store_true', help='do not perform zip integrity check')
    g2 = parser.add_mutually_exclusive_group()
    g2.add_argument('-delete', action='store_true', help='delete any files which fail integrity test')
    g2.add_argument('-noclean', action='store_true', help='leave any files which fail integrity test in place')
    g2.add_argument('-clean', action='store_true', help='(deprecated) default behaviour')
    g3 = add_id_filters(parser)
    g3.add_argument('-id', action='store', help='(deprecated) id or title of game to verify')
    parser.add_argument('-skipfiles', action='store', help='file name (or glob patterns) to NOT verify', nargs='*', default=[])
    add_os_filters(parser)
    add_lang_filters(parser)
    g6 = parser.add_mutually_exclusive_group()
    g6.add_argument('-skipextras', action='store_true', help='skip verification of any GOG extra files')
    g6.add_argument('-skipgames', action='store_true', help='skip verification of any GOG game files')
    add_installer_type_flags(parser)
    add_common_flags(parser)

def add_clean_command(subparsers):
    """Add clean command arguments"""
    parser = subparsers.add_parser('clean', help='Clean games directory of files not in manifest')
    parser.add_argument('cleandir', action='store', help='root directory containing gog games to be cleaned')
    parser.add_argument('-dryrun', action='store_true', help='display what would be cleaned, do not move files')
    add_common_flags(parser)

def add_clear_partial_downloads_command(subparsers):
    """Add clear_partial_downloads command arguments"""
    parser = subparsers.add_parser('clear_partial_downloads', help='Remove all partially downloaded files')
    parser.add_argument('gamedir', action='store', help='root directory containing gog games')
    parser.add_argument('-dryrun', action='store_true', help='display what would be cleaned, do not move files')
    add_common_flags(parser)

def add_trash_command(subparsers):
    """Add trash command arguments"""
    parser = subparsers.add_parser('trash', help='Permanently remove orphaned files (removes all unless parameters set)')
    parser.add_argument('gamedir', action='store', help='root directory containing gog games')
    parser.add_argument('-dryrun', action='store_true', help='display what would be trashed, do not delete files')
    parser.add_argument('-installersonly', action='store_true', help='(Deprecated) alias for -installers')
    parser.add_argument('-installers', action='store_true', help='delete file types used as installers')
    parser.add_argument('-images', action='store_true', help='delete !images subfolders')
    add_common_flags(parser)

def process_argv(argv):
    description = '''
GOG game downloader and backup tool
Downloads and maintains a local backup of your GOG games library.

MULTI-USER SUPPORT:
  Use --user <name> before the command to manage separate accounts.
  Each user has their own token and manifest, but shares the games directory
  to avoid duplicate downloads. Without --user, uses the default account.
  
  Note: If you specify a target directory (e.g., 'download /path/to/games'),
  that overrides the default shared directory behavior.
    '''
    
    epilog = '''
EXAMPLES:
  Login to GOG:
    %(prog)s login myemail@example.com
    
  Login with a specific user profile:
    %(prog)s --user alice login
    
  Update manifest with all games:
    %(prog)s update -full
    
  Update manifest for specific games:
    %(prog)s update -ids "witcher 3" "cyberpunk 2077"
    
  Update for a specific user:
    %(prog)s --user alice update -full
    
  Download all games:
    %(prog)s download
    
  Download for a specific user:
    %(prog)s --user bob download
    
  Download specific games:
    %(prog)s download -ids "witcher 3" beat_cop
    
  Download only Windows installers:
    %(prog)s download -os windows
    
  Download excluding extras:
    %(prog)s download -skipextras
    
  Download specific languages:
    %(prog)s download -lang en de
    
  Dry-run to see what would be downloaded:
    %(prog)s download -ids "witcher 3" -dryrun
    
  Verify downloaded files:
    %(prog)s verify
    
  Import existing files by MD5:
    %(prog)s import /path/to/files games
    
  Clean orphaned files:
    %(prog)s clean games

MULTI-USER EXAMPLES:
  Separate family accounts sharing games directory:
    %(prog)s --user alice login
    %(prog)s --user alice update -full
    %(prog)s --user alice download
    
    %(prog)s --user bob login
    %(prog)s --user bob download
    
  Note: Both users share the 'games' directory, so if Alice downloads 
  Witcher 3, Bob won't need to download it again if he also owns it.
  
  Using separate directories per user (overrides shared directory):
    %(prog)s --user alice download /mnt/alice_games
    %(prog)s --user bob download /mnt/bob_games

COMMON OPTIONS:
  --user NAME              Use named user profile (creates users/<name>/ directory)
  -ids GAME [GAME ...]     Specify games by ID or title (can be partial match)
  -skipids GAME [GAME ...] Skip specific games
  -os windows linux mac    Filter by operating system
  -lang en de fr ...       Filter by language
  -dryrun                  Show what would happen without doing it
  -nolog                   Don't write to gogrepo.log
  -debug                   Enable debug output

For detailed help on a specific command:
  %(prog)s COMMAND -h
  Example: %(prog)s download -h
    '''
    
    p1 = argparse.ArgumentParser(
        prog='gogrepoc_new.py',
        description=description,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False
    )
    sp1 = p1.add_subparsers(help='command', dest='command', title='commands')
    sp1.required = True

    # Add all commands
    add_login_command(sp1)
    add_update_command(sp1)
    add_download_command(sp1)
    add_import_command(sp1)
    add_backup_command(sp1)
    add_verify_command(sp1)
    add_clean_command(sp1)
    add_clear_partial_downloads_command(sp1)
    add_trash_command(sp1)

    # Other arguments
    g1 = p1.add_argument_group('other')
    g1.add_argument('--user', help='user profile/account ID (for multi-user setups)', default=None)
    g1.add_argument('-h', '--help', action='help', help='show help message and exit')
    g1.add_argument('-v', '--version', action='version', help='show version number and exit',
                    version="%s (version %s)" % (__appname__, __version__))

    # parse the given argv.  raises SystemExit on error
    args = p1.parse_args(argv[1:])
    
    # Validate user_id if provided
    if args.user:
        try:
            validate_user_id(args.user)
        except ValueError as e:
            error(str(e))
            sys.exit(1)
    
    if not args.nolog:
        rootLogger.addHandler(loggingHandler)
        
    if not args.debug:     
        rootLogger.setLevel(logging.INFO)

    if args.command == 'update' or args.command == 'download' or args.command == 'backup' or args.command == 'import' or args.command == 'verify':
        for lang in args.lang+args.skiplang:  # validate the language
            if lang not in VALID_LANG_TYPES:
                error('error: specified language "%s" is not one of the valid languages %s' % (lang, VALID_LANG_TYPES))
                raise SystemExit(1)

        for os_type in args.os+args.skipos:  # validate the os type
            if os_type not in VALID_OS_TYPES:
                error('error: specified os "%s" is not one of the valid os types %s' % (os_type, VALID_OS_TYPES))
                raise SystemExit(1)
                
    return args

def main(args):
    stime = datetime.datetime.now()

    if args.command == 'login':
        cmd_login(args.username, args.password, user_id=args.user)
        return  # no need to see time stats
    elif args.command == 'update':
        if not args.os:    
            if args.skipos:
                args.os = [x for x in VALID_OS_TYPES if x not in args.skipos]
            else:
                args.os = DEFAULT_OS_LIST
        if not args.lang:    
            if args.skiplang:
                args.lang = [x for x in VALID_LANG_TYPES if x not in args.skiplang]
            else:
                args.lang = DEFAULT_LANG_LIST
        if (not args.skipknown) and (not args.updateonly) and (not args.standard):         
            if (args.ids):
                args.full = True
        if args.wait > 0.0:
            info('sleeping for %.2fhr...' % args.wait)
            time.sleep(args.wait * 60 * 60)                
        if not args.installers:
            args.installers = "standalone"
        cmd_update(args.os, args.lang, args.skipknown, args.updateonly, not args.full, args.ids, args.skipids,args.skiphidden,args.installers,args.resumemode,args.strictverify,args.strictdupe,args.lenientdownloadsupdate,args.strictextrasupdate,args.md5xmls,args.nochangelogs)
    elif args.command == 'download':
        if (args.id):
            args.ids = [args.id]
        if not args.os:    
            if args.skipos:
                args.os = [x for x in VALID_OS_TYPES if x not in args.skipos]
            else:
                args.os = [x for x in VALID_OS_TYPES]
        if not args.lang:    
            if args.skiplang:
                args.lang = [x for x in VALID_LANG_TYPES if x not in args.skiplang]
            else:
                args.lang = [x for x in VALID_LANG_TYPES]
        if args.skipgames:
            args.skipstandalone = True
            args.skipgalaxy = True
            args.skipshared = True
        if args.wait > 0.0:
            info('sleeping for %.2fhr...' % args.wait)
            time.sleep(args.wait * 60 * 60)
        if args.downloadlimit is not None:
            args.downloadlimit = args.downloadlimit*1024.0*1024.0 #Convert to Bytes
        cmd_download(args.savedir, args.skipextras, args.skipids, args.dryrun, args.ids,args.os,args.lang,args.skipgalaxy,args.skipstandalone,args.skipshared, args.skipfiles,args.covers,args.backgrounds,args.skippreallocation,not args.nocleanimages,args.downloadlimit)
    elif args.command == 'import':
        args.skipgames = False
        args.skipextras = False
        if not args.os:  
            if args.skipos:
                args.os = [x for x in VALID_OS_TYPES if x not in args.skipos]
            else:
                args.os = VALID_OS_TYPES
        if not args.lang:    
            if args.skiplang:
                args.lang = [x for x in VALID_LANG_TYPES if x not in args.skiplang]
            else:
                args.lang = VALID_LANG_TYPES  
        if args.skipgames:
            args.skipstandalone = True
            args.skipgalaxy = True
            args.skipshared = True
        cmd_import(args.src_dir, args.dest_dir,args.os,args.lang,args.skipextras,args.skipids,args.ids,args.skipgalaxy,args.skipstandalone,args.skipshared,False)
    elif args.command == 'verify':
        #Hardcode these as false since extras currently do not have MD5s as such skipgames would give nothing and skipextras would change nothing. The logic path and arguments are present in case this changes, though commented out in the case of arguments)
        if args.clean:
            warn("The -clean option is deprecated, as the default behaviour has been changed to clean files that fail the verification checks. -noclean now exists for leaving files in place. Please update your scripts accordingly. ")
        if (args.id):
            args.ids = [args.id]    
        if not args.os:    
            if args.skipos:
                args.os = [x for x in VALID_OS_TYPES if x not in args.skipos]
            else:
                args.os = VALID_OS_TYPES
        if not args.lang:    
            if args.skiplang:
                args.lang = [x for x in VALID_LANG_TYPES if x not in args.skiplang]
            else:
                args.lang = VALID_LANG_TYPES
        if args.skipgames:
            args.skipstandalone = True
            args.skipgalaxy = True
            args.skipshared = True                
        check_md5 = not args.skipmd5
        check_filesize = not args.skipsize
        check_zips = not args.skipzip
        cmd_verify(args.gamedir, args.skipextras,args.skipids,check_md5, check_filesize, check_zips, args.delete,not args.noclean,args.ids,  args.os, args.lang,args.skipgalaxy,args.skipstandalone,args.skipshared, args.skipfiles, args.forceverify,args.permissivechangeclear)
    elif args.command == 'backup':
        if not args.os:    
            if args.skipos:
                args.os = [x for x in VALID_OS_TYPES if x not in args.skipos]
            else:
                args.os = VALID_OS_TYPES
        if not args.lang:    
            if args.skiplang:
                args.lang = [x for x in VALID_LANG_TYPES if x not in args.skiplang]
            else:
                args.lang = VALID_LANG_TYPES
        if args.skipgames:
            args.skipstandalone = True
            args.skipgalaxy = True
            args.skipshared = True
        cmd_backup(args.src_dir, args.dest_dir,args.skipextras,args.os,args.lang,args.ids,args.skipids,args.skipgalaxy,args.skipstandalone,args.skipshared)
    elif args.command == 'clear_partial_downloads':
        cmd_clear_partial_downloads(args.gamedir,args.dryrun)
    elif args.command == 'clean':
        cmd_clean(args.cleandir, args.dryrun)
    elif args.command == "trash":
        if (args.installersonly):
            args.installers = True
        cmd_trash(args.gamedir,args.installers,args.images,args.dryrun)

    etime = datetime.datetime.now()
    info('--')
    info('total time: %s' % (etime - stime))

if __name__ == "__main__":
    try:
        wakelock = Wakelock()
        wakelock.take_wakelock()
        main(process_argv(sys.argv))
        info('exiting...')
    except KeyboardInterrupt:
        info('exiting...')
        sys.exit(1)
    except SystemExit:
        raise
    except Exception:
        log_exception('fatal...')
        sys.exit(1)
    finally:
        wakelock.release_wakelock()
