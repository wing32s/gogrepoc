# GOG Collection Sync Tool

Python-based tool for downloading and maintaining a local backup of your GOG.com game library and extras.

This repository is based on [Kalanyr/gogrepoc](https://github.com/Kalanyr/gogrepoc), but the active codebase here has been refactored and extended. The maintained `gogrepoc.py` entrypoint adds modularization, improved help, automatic token renewal, and a menu-driven wrapper.

License: GPLv3+

## Features

- Choose which games to process by OS, language, game ID, or title.
- Save `!info.txt` per game with game and file metadata.
- Save `!serial.txt` when a game includes a serial or key.
- Verify downloaded files using size, MD5, and zip integrity checks.
- Resume interrupted downloads where possible.
- Import an existing local collection by matching known files.
- Keep a reusable manifest of your library for update and download workflows.
- Use browser-based login with token refresh support in the refactored version.
- Use an interactive menu wrapper if you do not want to remember switches.

## What's New In This Fork

- The original monolithic script has been split into focused modules:
  - `modules/api.py`
  - `modules/download.py`
  - `modules/manifest.py`
  - `modules/commands.py`
  - `modules/utils.py`
- `gogrepoc.py` adds:
  - automatic token renewal
  - improved CLI help and examples
  - cleaner progress display
  - easier testing and maintenance
- `gogrepoc_menu.py` provides an interactive text menu that builds and runs commands for you.

## Quick Start

If you want a guided interface instead of remembering switches:

```bash
python gogrepoc_menu.py
```

Typical command-line flow:

```bash
python gogrepoc.py login
python gogrepoc.py update -full
python gogrepoc.py download
python gogrepoc.py verify
```

## Common Workflows

Update your manifest with all owned games:

```bash
python gogrepoc.py update -full
```

Update only new games in your library:

```bash
python gogrepoc.py update -skipknown
```

Update only games marked as updated:

```bash
python gogrepoc.py update -updateonly
```

Update specific games:

```bash
python gogrepoc.py update -ids trine_2_complete_story
```

Download specific games:

```bash
python gogrepoc.py download -ids trine_2_complete_story
```

Download only specific operating systems or languages:

```bash
python gogrepoc.py download -os windows
python gogrepoc.py download -lang en de
```

Preview downloads without downloading:

```bash
python gogrepoc.py download -dryrun
```

Skip extras:

```bash
python gogrepoc.py download -skipextras
```

Verify downloaded files:

```bash
python gogrepoc.py verify
```

Import an existing collection:

```bash
python gogrepoc.py import /path/to/source games
```

Clean orphaned files:

```bash
python gogrepoc.py clean games
```

## Commands

### `login`

Authenticate with GOG using browser-based login and save the token locally.

```bash
python gogrepoc.py login
```

### `update`

Refresh the local manifest with games and file metadata from GOG.

Common switches:

- `-full` update all games on the account
- `-skipknown` only add games not already in the manifest
- `-updateonly` only refresh games marked as updated
- `-ids ...` include only specific games
- `-skipids ...` exclude specific games
- `-os windows linux mac`
- `-lang en de fr ...`
- `-skiphidden`
- `-resumemode noresume|resume|onlyresume`
- `-installers standalone|both`
- `-wait HOURS`

### `download`

Download games and extras using the saved manifest.

Common switches:

- `[savedir]` optional target directory
- `-dryrun`
- `-skipextras`
- `-ids ...`
- `-skipids ...`
- `-os windows linux mac`
- `-lang en de fr ...`
- `-skipgalaxy`
- `-skipstandalone`
- `-skipshared`
- `-skipfiles ...`
- `-covers`
- `-backgrounds`
- `-downloadlimit MB`
- `-wait HOURS`

### `verify`

Verify downloaded files against the manifest.

Common switches:

- `[gamedir]` optional games directory
- `-skipmd5`
- `-skipsize`
- `-skipzip`
- `-delete`
- `-noclean`
- `-forceverify`
- `-ids ...`
- `-skipids ...`
- `-os windows linux mac`
- `-lang en de fr ...`
- `-skipgalaxy`
- `-skipstandalone`
- `-skipshared`

### `import`

Import matching files from an existing collection into the managed directory.

```bash
python gogrepoc.py import SRC_DIR DEST_DIR
```

### `backup`

Copy known files from one managed collection to another location.

```bash
python gogrepoc.py backup SRC_DIR DEST_DIR
```

### `clean`

Move files not known by the manifest into the orphaned area.

```bash
python gogrepoc.py clean GAMES_DIR
```

### `clear_partial_downloads`

Remove partial download files.

```bash
python gogrepoc.py clear_partial_downloads GAMES_DIR
```

### `trash`

Permanently remove orphaned files.

```bash
python gogrepoc.py trash GAMES_DIR
```

For full option details on any command:

```bash
python gogrepoc.py -h
python gogrepoc.py download -h
```

## Installation

```bash
git clone https://github.com/wing32s/gogrepoc.git
cd gogrepoc
pip install -r requirements.txt
```

Optional test setup:

```bash
pip install -r tests/requirements.txt
pytest tests/
```

## Requirements

- Python 3.10+
- `requests`
- `html5lib`
- `psutil`

Optional:

- `html2text` for prettier changelog text output

## Project Status

This repository ships the refactored implementation as `gogrepoc.py`. The original upstream monolithic script is not carried here; if you want the upstream version, use the project link in the credits section.

## Credits

Original project: [gogrepoc by Kalanyr](https://github.com/Kalanyr/gogrepoc)  
Based on: [gogrepo by eddie3](https://github.com/eddie3/gogrepo)

## License

See [LICENSE](LICENSE).
