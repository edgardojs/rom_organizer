# ROM Organizer

A safe, offline-first tool for scanning, hashing, deduplicating, normalizing, and organizing ROM files — with archive inspection and DAT-based identification. No internet access required. Zero third-party dependencies.

## Features

- **Recursive scanning** of any ROM library directory
- **SHA-256 / MD5 / CRC-32 hashing** for exact duplicate detection and DAT matching
- **Parallel hashing** via `ProcessPoolExecutor` for large libraries
- **Archive inspection** — catalogs contents of `.zip` and `.7z` files (critical for arcade ROMs)
- **DAT file matching** — load No-Intro, Redump, and MAME/FBNeo DAT files to identify ROMs by hash
- **Possible duplicate detection** via normalized filename + file size matching
- **Filename normalization** with safe, reversible rules
- **Rule-based sorting** into system folders (NES, SNESIS, Genesis, etc.)
- **Extension priority** — resolve ambiguous extensions like `.bin` and `.iso` via config
- **Quarantine** for duplicates (never auto-deletes)
- **Dry-run mode** for all operations
- **Full audit trail** — every action is logged and reversible
- **Selective rollback** — undo specific actions by ID or the last N actions
- **Database backup** — automatic `.bak` copy before any mutation
- **Post-move checksum verification** — re-hashes files after moving to confirm integrity
- **Atomic cross-filesystem moves** — safe moves across mount points
- **SQLite database** with WAL mode and schema versioning
- **Zero third-party dependencies** (Python 3.11+ stdlib only)

## Quick Start

```bash
# 1. Scan your ROM library and hash all files
python3 main.py --config config.json scan "/media/ROMS"

# 2. View the scan report
python3 main.py --config config.json report

# 3. (Optional) Load DAT files for identification
python3 main.py --config config.json load-dats /path/to/dat/files

# 4. (Optional) Identify scanned files against DAT entries
python3 main.py --config config.json identify

# 5. Preview filename normalization (dry-run by default)
python3 main.py --config config.json normalize --dry-run

# 6. Preview file organization (dry-run by default)
python3 main.py --config config.json organize --dry-run

# 7. Apply organization for real
python3 main.py --config config.json organize --apply
```

## Setup

### Requirements

- Python 3.11 or later
- No pip packages needed — uses only the standard library
- **Optional**: `7z` command-line tool (for `.7z` archive inspection)

### Configuration

Copy and edit the sample config:

```bash
cp config.json my_config.json
# Edit rom_root, extensions, extension_map, etc.
```

Key configuration options:

| Option | Default | Description |
|--------|---------|-------------|
| `rom_root` | `""` | Root directory to scan (can also pass via CLI) |
| `extensions` | See config | List of ROM file extensions to detect |
| `exclude_dirs` | `["__MACOSX", ".git", ".DS_Store"]` | Directory names to skip |
| `min_file_size` | `1024` | Minimum file size in bytes to include (default 1 KiB) |
| `hash_algorithm` | `"sha256"` | Hash algorithm (must be in `hashlib`) |
| `hash_workers` | `1` | Number of parallel hashing workers (1 = sequential) |
| `output_dir` | `"rom_organizer_output"` | Where reports and organized files go |
| `quarantine_subdir` | `"quarantine"` | Subfolder for duplicate files |
| `extension_map` | See config | Maps extensions to system folder names |
| `extension_priority` | `{".bin": "genesis", ".iso": "ps2"}` | Priority overrides for ambiguous extensions |
| `inspect_archives` | `true` | Whether to inspect `.zip`/`.7z` archive contents |
| `dat_dir` | `""` | Default directory for DAT files (overridable via CLI) |
| `separator_chars` | `"_-+"` | Characters to treat as word separators |
| `conflict_suffix_template` | `"_{counter}"` | Suffix for resolving name conflicts |

## CLI Commands

### `scan <rom_root>`

Scans a directory recursively, hashes all ROM files, inspects archives, detects duplicates, and generates a report.

```bash
python3 main.py --config config.json scan "/path/to/roms"
```

### `report`

Generates a report from existing scan data (no re-scan needed).

```bash
python3 main.py --config config.json report
```

### `load-dats [dat_dir]`

Loads DAT files (No-Intro, Redump, or MAME/FBNeo XML format) into the database for ROM identification. Accepts an optional directory argument; falls back to `dat_dir` from config.

```bash
python3 main.py --config config.json load-dats /path/to/dat/files
```

### `identify`

Matches scanned files against loaded DAT entries using SHA-256, MD5, and CRC-32 hashes. Updates file records with identified game name, description, and system.

```bash
python3 main.py --config config.json identify
```

### `normalize`

Computes normalized filenames. Dry-run by default.

```bash
# Preview only
python3 main.py --config config.json normalize --dry-run

# Actually rename files
python3 main.py --config config.json normalize --apply
```

### `organize`

Proposes and optionally applies file organization actions:
- Rename files to normalized names
- Move exact duplicates to quarantine
- Move files into system-based folders

```bash
# Preview only (default)
python3 main.py --config config.json organize --dry-run

# Apply for real
python3 main.py --config config.json organize --apply
```

### `rollback`

Reverses applied actions by moving files back to their original locations. Supports selective rollback.

```bash
# Roll back all applied actions
python3 main.py --config config.json rollback

# Roll back only the last 5 actions
python3 main.py --config config.json rollback --last 5

# Roll back a specific action by ID
python3 main.py --config config.json rollback --action-id 42
```

### `review`

Interactive review of scan findings. Walks through corrupted files, duplicates, and proposed actions one by one, asking for your approval before making changes.

```bash
# Preview everything (dry-run — no files are modified)
python3 main.py --config config.json review

# Actually apply approved changes
python3 main.py --config config.json review --apply

# Review only corrupted/error files
python3 main.py --config config.json review --corrupted

# Review only duplicate groups
python3 main.py --config config.json review --duplicates

# Review only proposed actions (renames, moves, quarantines)
python3 main.py --config config.json review --actions
```

During review, you'll see each item and can choose to:
- **Corrupted files**: `[m]ove` to corrupted folder, `[s]kip`, `[M]ove all`, `[S]kip all`, `[q]uit`
- **Duplicates**: `[q]uarantine` non-canonical copies, `[k]eep all`, `[Q]uarantine all`, `[K]eep all`, `[s]kip`
- **Proposed actions**: `[a]pply`, `[s]kip`, `[A]pply all`, `[S]kip all`, `[q]uit`

> **Safety**: Corrupted files are **moved** to a `corrupted/` subfolder, never deleted. All actions are reversible via `rollback`.

## How It Works

### Archive Inspection

ROM archives (`.zip`, `.7z`) — especially arcade ROMs from MAME and FBNeo — contain multiple small ROM binaries inside. The organizer inspects these archives during scanning:

- **`.zip` files**: Inspected using Python's `zipfile` module. Each entry's name, size, CRC-32, and SHA-256 (for entries under 64 MiB) are recorded.
- **`.7z` files**: Inspected via the `7z` command-line tool. Requires `7z` to be installed and on `PATH`.
- **Archive fingerprints**: A deterministic hash of sorted `(name, sha256)` pairs is computed for each archive, enabling reliable duplicate detection even when file ordering differs.

### DAT-Based Identification

The organizer can match your ROMs against known databases:

1. **Load DAT files** — No-Intro (cartridge systems), Redump (disc systems), and MAME/FBNeo (arcade) XML formats are supported.
2. **Identify ROMs** — Scanned files are matched by SHA-256, MD5, and CRC-32 hashes against the loaded DAT entries.
3. **Results** — Matched files are annotated with game name, description, and system in the database.

```bash
# Load DAT files
python3 main.py --config config.json load-dats ~/dat/

# Then identify scanned files
python3 main.py --config config.json identify
```

### Duplicate Detection

1. **Exact duplicates**: Files with identical SHA-256 hashes. The first file (alphabetically) is kept as canonical; others are proposed for quarantine.

2. **Possible duplicates**: Files with matching normalized filenames and identical file sizes, but different hashes. These might be different dumps of the same ROM (e.g., different regions or versions).

### Filename Normalization

Rules applied (in order):
1. Trim leading/trailing whitespace
2. Replace separator characters (`_`, `-`, `+`) with spaces
3. Collapse multiple spaces into one
4. Clean up spacing around parentheses and brackets
5. Title-case names that are ALL CAPS or all lowercase
6. Preserve region tags like `(U)`, `(J)`, `(Europe)`
7. Preserve version tags like `(Rev A)`, `(V1.1)`
8. Preserve GoodTools tags like `[!]`, `[b1]`, `[o1]`
9. Preserve original extension (lowercased)

**Examples:**
| Original | Normalized |
|----------|-----------|
| `Super_Mario_Bros_(U).nes` | `Super Mario Bros (U).nes` |
| `  Super  Mario  Bros  (U).nes` | `Super Mario Bros (U).nes` |
| `SUPER MARIO BROS (U).nes` | `Super Mario Bros (U).nes` |
| `Zelda+3+(U).sfc` | `Zelda 3 (U).sfc` |

### Extension Priority

Some extensions are ambiguous — `.bin` is used by both Genesis and PS1, `.iso` by both PS2 and PSP. The `extension_priority` config resolves this:

```json
{
  "extension_priority": {
    ".bin": "genesis",
    ".iso": "ps2"
  }
}
```

This tells the sorter to treat `.bin` files as Genesis ROMs and `.iso` files as PS2 ROMs, overriding the default `extension_map`.

### File Organization

Files are proposed for moves into system folders based on extension:

```
rom_organizer_output/
├── nes/
├── snes/
├── genesis/
├── gameboy/
├── gba/
├── n64/
├── ps1/
├── ps2/
├── arcade/
├── quarantine/    ← duplicates go here
└── logs/
```

## Rollback Guide

Every file operation is recorded in the `proposed_actions` database table with:
- Source path (original location)
- Proposed path (new location)
- Whether the action was applied
- Whether it was rolled back

### To roll back the last organize run:

```bash
python3 main.py --config config.json rollback
```

### Selective rollback:

```bash
# Roll back only the last 5 actions
python3 main.py --config config.json rollback --last 5

# Roll back a specific action by ID
python3 main.py --config config.json rollback --action-id 42
```

### To manually roll back:

1. Open the SQLite database: `sqlite3 rom_organizer.db`
2. Query applied actions:
   ```sql
   SELECT id, action_type, source_path, proposed_path
   FROM proposed_actions
   WHERE applied = 1 AND rolled_back = 0;
   ```
3. Move files back manually using the `source_path` column.
4. Mark actions as rolled back:
   ```sql
   UPDATE proposed_actions SET rolled_back = 1 WHERE id = <action_id>;
   ```

### Log files

Every action is logged to `rom_organizer_output/logs/rom_organizer_YYYYMMDD_HHMMSS.log`. These logs contain the full audit trail and can be used for manual recovery.

## Database Schema

| Table | Purpose |
|-------|---------|
| `files` | Inventory of all ROM files with paths, names, hashes, sizes, DAT info |
| `hashes` | Index of SHA-256 hashes for fast duplicate lookup |
| `archive_entries` | Files inside `.zip`/`.7z` archives (name, size, CRC-32, SHA-256) |
| `dat_files` | Loaded DAT files (filename, name, description, category, version) |
| `dat_games` | Game entries from DAT files (name, description, system, clone info) |
| `dat_roms` | Individual ROM entries with hashes (SHA-256, MD5, CRC-32) |
| `duplicate_groups` | Groups of exact or possible duplicates |
| `duplicate_group_members` | Files belonging to each duplicate group |
| `proposed_actions` | All proposed/applied/rolled-back file operations |

The database uses **schema versioning** (currently v2) with automatic migration from v1. It runs in **WAL mode** for safe concurrent reads and uses **batch transactions** for bulk operations.

## Testing

```bash
# Generate fake ROM files for testing
python3 test_data_generator.py --output-dir /tmp/test_roms

# Run the organizer on test data
python3 main.py --config config.json scan /tmp/test_roms
python3 main.py --config config.json organize --dry-run

# Run the full test suite
python3 -m unittest discover -s . -p "test_*.py" -v
```

## Safety Guarantees

- ✅ **Never deletes files** — duplicates are moved to quarantine, not deleted
- ✅ **Dry-run by default** — all operations require `--apply` to make changes
- ✅ **Full audit trail** — every action logged with timestamps
- ✅ **Reversible** — rollback command reverses applied actions (supports selective rollback)
- ✅ **Offline-first** — no internet access needed, no data sent anywhere
- ✅ **Conservative** — doesn't assume ROM authenticity from filenames
- ✅ **Checksum verification** — files are re-hashed after moving to confirm integrity
- ✅ **Atomic moves** — safe cross-filesystem moves with temp-file + rename fallback
- ✅ **Database backup** — automatic `.bak` copy before any mutation
- ✅ **Path traversal protection** — all target paths validated before file operations

## Project Structure

```
rom_organizer/
├── main.py               # CLI entry point with argparse subcommands
├── scanner.py            # Recursive directory scanning + archive inspection
├── hasher.py              # SHA-256 file hashing (sequential or parallel)
├── normalizer.py          # Filename normalization rules
├── sorter.py              # Duplicate detection and file organization
├── database.py            # SQLite database layer (schema v2, transactions)
├── reporter.py            # Report generation
├── reviewer.py            # Interactive review of findings (corrupted, dupes, actions)
├── config.py              # Configuration management (dataclass + JSON)
├── progress.py            # Progress bar and folder size estimation
├── archiver.py            # Archive inspection (.zip and .7z)
├── dat_parser.py           # DAT file parser (No-Intro, Redump, MAME)
├── config.json            # Sample configuration file
├── test_data_generator.py  # Creates fake ROM files for testing
├── test_archiver.py        # Tests for archive inspection
├── test_config.py          # Tests for configuration
├── test_database.py        # Tests for database layer
├── test_dat_parser.py       # Tests for DAT parsing
├── test_hasher.py           # Tests for hashing
├── test_integration.py      # Integration tests (full pipeline)
├── test_normalizer.py       # Tests for normalization
├── test_progress.py         # Tests for progress bar and estimation
├── test_reviewer.py         # Tests for interactive review
├── test_sorter.py           # Tests for sorting/organizing
└── README.md               # This file
```

## License

This tool is provided as-is for personal ROM library management. Use responsibly and in accordance with applicable laws regarding game backups.