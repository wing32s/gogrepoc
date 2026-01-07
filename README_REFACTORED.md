# GOG Collection Sync Tool - Refactored

This is a refactored and enhanced version of [gogrepoc](https://github.com/Kalanyr/gogrepoc), a Python tool for backing up your GOG.com game library.

## What's Changed

### Modularization
The original 2000+ line monolithic `gogrepoc.py` has been split into focused modules:
- `modules/api.py` - GOG API interactions
- `modules/download.py` - Download logic
- `modules/manifest.py` - Manifest management
- `modules/commands.py` - Command implementations
- `modules/utils.py` - Shared utilities

### New Features
1. **Automatic Token Renewal** - Sessions no longer expire after 1 hour
2. **Provisional File Validation** - Validates incomplete downloads before moving
3. **Enhanced Help** - Comprehensive `--help` with examples
4. **Better Progress Display** - Cleaner terminal output

### Testing
Full test suite validates that refactored code produces identical results to the original:
```bash
pip install -r tests/requirements.txt
pytest tests/
```

### Why This Matters
- **Testable** - Functions can now be unit tested
- **Maintainable** - Each module has a single responsibility
- **Debuggable** - Easier to trace issues to specific modules
- **Extensible** - New features can be added without touching everything

## Installation

```bash
# Clone this repository
git clone https://github.com/YOUR_USERNAME/gog-sync.git
cd gog-sync

# Install dependencies
pip install -r requirements.txt

# Run tests (optional but recommended)
pip install -r tests/requirements.txt
pytest tests/
```

## Usage

Everything works the same as the original, plus improvements:

```bash
# Login (token now auto-renews!)
python gogrepoc_new.py login

# Update manifest
python gogrepoc_new.py update -full

# Download games
python gogrepoc_new.py download

# Better help
python gogrepoc_new.py -h
python gogrepoc_new.py download -h
```

## Backward Compatibility

The original `gogrepoc.py` is still included and unchanged. You can use either:
- `python gogrepoc.py` - Original version
- `python gogrepoc_new.py` - Refactored version

Both read the same manifest and download files, so you can switch between them.

## Contributing Back

This refactoring is intended to be contributed back to the original project. The test suite proves the refactored code produces identical results while being much more maintainable.

## Credits

Original project: [gogrepoc by Kalanyr](https://github.com/Kalanyr/gogrepoc)
Based on: [gogrepo by eddie3](https://github.com/eddie3/gogrepo)

## License

GPLv3+ (same as original)
