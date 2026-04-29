"""SQLite database layer for the ROM Organizer.

Design decision: We use a single SQLite database per scan session rather than
a persistent daemon. This keeps things simple, offline-first, and auditable.
The schema is versioned so future migrations are straightforward.

All database operations go through this module so the rest of the codebase
never touches raw SQL.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 2

# ── Schema DDL ────────────────────────────────────────────────────────────

_SCHEMA_SQL = """
-- Metadata table for schema versioning.
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

-- Core file inventory.
CREATE TABLE IF NOT EXISTS files (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    path            TEXT NOT NULL UNIQUE,
    original_name   TEXT NOT NULL,
    normalized_name TEXT,
    extension       TEXT NOT NULL,
    size            INTEGER NOT NULL,
    sha256          TEXT,
    md5             TEXT,
    crc32           TEXT,
    scan_timestamp  TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'scanned',
    -- status values: scanned | normalized | organized | quarantined | verified
    notes           TEXT,
    dat_game_name   TEXT,
    dat_description TEXT,
    dat_system      TEXT,
    is_archive      INTEGER NOT NULL DEFAULT 0,
    archive_fingerprint TEXT
);

-- Hash index for fast duplicate lookups.
CREATE TABLE IF NOT EXISTS hashes (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    sha256   TEXT NOT NULL,
    file_id  INTEGER NOT NULL,
    FOREIGN KEY (file_id) REFERENCES files(id),
    UNIQUE(sha256, file_id)
);
CREATE INDEX IF NOT EXISTS idx_hashes_sha256 ON hashes(sha256);

-- Archive entry inventory (files inside .zip/.7z archives).
CREATE TABLE IF NOT EXISTS archive_entries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id         INTEGER NOT NULL,
    entry_name      TEXT NOT NULL,
    entry_size      INTEGER NOT NULL,
    compressed_size INTEGER NOT NULL DEFAULT 0,
    crc32           TEXT,
    sha256          TEXT,
    FOREIGN KEY (file_id) REFERENCES files(id)
);
CREATE INDEX IF NOT EXISTS idx_archive_entries_file_id ON archive_entries(file_id);
CREATE INDEX IF NOT EXISTS idx_archive_entries_sha256 ON archive_entries(sha256);

-- DAT file registry (which DATs have been loaded).
CREATE TABLE IF NOT EXISTS dat_files (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    filename    TEXT NOT NULL UNIQUE,
    name        TEXT,
    description TEXT,
    category    TEXT,
    version     TEXT,
    loaded_at   TEXT NOT NULL
);

-- DAT game entries (from parsed DAT files).
CREATE TABLE IF NOT EXISTS dat_games (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    dat_id        INTEGER NOT NULL,
    game_name     TEXT NOT NULL,
    description   TEXT,
    category      TEXT,
    clone_of      TEXT,
    year          TEXT,
    manufacturer  TEXT,
    FOREIGN KEY (dat_id) REFERENCES dat_files(id)
);
CREATE INDEX IF NOT EXISTS idx_dat_games_dat_id ON dat_games(dat_id);
CREATE INDEX IF NOT EXISTS idx_dat_games_name ON dat_games(game_name);

-- DAT ROM entries (individual ROMs within a game).
CREATE TABLE IF NOT EXISTS dat_roms (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id      INTEGER NOT NULL,
    rom_name     TEXT NOT NULL,
    size         INTEGER NOT NULL,
    crc32        TEXT,
    md5          TEXT,
    sha256       TEXT,
    merge_name   TEXT,
    FOREIGN KEY (game_id) REFERENCES dat_games(id)
);
CREATE INDEX IF NOT EXISTS idx_dat_roms_sha256 ON dat_roms(sha256);
CREATE INDEX IF NOT EXISTS idx_dat_roms_md5 ON dat_roms(md5);
CREATE INDEX IF NOT EXISTS idx_dat_roms_crc32 ON dat_roms(crc32);

-- Groups of duplicate files.
CREATE TABLE IF NOT EXISTS duplicate_groups (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    group_type  TEXT NOT NULL,  -- 'exact' or 'possible'
    sha256      TEXT,           -- NULL for possible duplicates
    created_at  TEXT NOT NULL
);

-- Link table: which files belong to which duplicate group.
CREATE TABLE IF NOT EXISTS duplicate_group_members (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id  INTEGER NOT NULL,
    file_id   INTEGER NOT NULL,
    is_canonical INTEGER NOT NULL DEFAULT 0,  -- 1 = keep this one
    FOREIGN KEY (group_id) REFERENCES duplicate_groups(id),
    FOREIGN KEY (file_id)  REFERENCES files(id),
    UNIQUE(group_id, file_id)
);

-- Proposed actions (rename, move, quarantine) — auditable trail.
CREATE TABLE IF NOT EXISTS proposed_actions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id         INTEGER NOT NULL,
    action_type     TEXT NOT NULL,  -- 'rename' | 'move' | 'quarantine'
    source_path     TEXT NOT NULL,
    proposed_path   TEXT NOT NULL,
    reason          TEXT,
    applied         INTEGER NOT NULL DEFAULT 0,
    applied_at      TEXT,
    rolled_back     INTEGER NOT NULL DEFAULT 0,
    rolled_back_at  TEXT,
    FOREIGN KEY (file_id) REFERENCES files(id)
);
"""


class Database:
    """Thin wrapper around SQLite for the ROM Organizer.

    Design decision: We open/close the connection per transaction rather than
    keeping a long-lived cursor. This is safer for a CLI tool that may crash
    between operations and ensures WAL mode works correctly.

    Supports use as a context manager for transaction batching:
        with db.transaction():
            db.upsert_file(...)
            db.update_file_hash(...)
            # commit happens once at the end
    """

    def __init__(self, db_path: Path) -> None:
        """Initialize the database connection and ensure schema exists.

        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._in_transaction = False
        self._ensure_schema()

    # ── Connection management ─────────────────────────────────────────

    def _connect(self) -> sqlite3.Connection:
        """Open a connection with WAL mode for better concurrency."""
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
        """Lazy connection property."""
        if self._conn is None:
            self._conn = self._connect()
        return self._conn

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ── Transaction support ────────────────────────────────────────────

    def transaction(self):
        """Context manager for batching multiple operations into one commit.

        Usage:
            with db.transaction():
                db.upsert_file(...)
                db.update_file_hash(...)
                # Single commit at exit instead of per-operation commits.
        """
        return _Transaction(self)

    def _commit_or_defer(self) -> None:
        """Commit immediately unless inside a transaction block."""
        if not self._in_transaction:
            self.conn.commit()

    # ── Schema bootstrap ───────────────────────────────────────────────

    def _ensure_schema(self) -> None:
        """Create tables if they don't exist and run migrations."""
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA_SQL)
            # Run migrations for existing databases.
            self._run_migrations(conn)
            # Stamp current version if not already present.
            row = conn.execute(
                "SELECT version FROM schema_version WHERE version = ?", (SCHEMA_VERSION,)
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
                    (SCHEMA_VERSION, datetime.now(timezone.utc).isoformat()),
                )
            conn.commit()
        finally:
            conn.close()

    def _run_migrations(self, conn: sqlite3.Connection) -> None:
        """Apply schema migrations for databases created with older versions."""
        # Get the current schema version.
        row = conn.execute(
            "SELECT MAX(version) FROM schema_version"
        ).fetchone()
        current_version = row[0] if row[0] is not None else 0

        if current_version < 2:
            self._migrate_v1_to_v2(conn)

    def _migrate_v1_to_v2(self, conn: sqlite3.Connection) -> None:
        """Migrate schema from v1 to v2: add archive/DAT columns and tables."""
        logger.info("Migrating database from v1 to v2...")

        # Add new columns to files table (if they don't exist).
        new_columns = [
            ("md5", "TEXT"),
            ("crc32", "TEXT"),
            ("dat_game_name", "TEXT"),
            ("dat_description", "TEXT"),
            ("dat_system", "TEXT"),
            ("is_archive", "INTEGER NOT NULL DEFAULT 0"),
            ("archive_fingerprint", "TEXT"),
        ]
        for col_name, col_type in new_columns:
            try:
                conn.execute(f"ALTER TABLE files ADD COLUMN {col_name} {col_type}")
            except sqlite3.OperationalError:
                pass  # Column already exists.

        # New tables are created by IF NOT EXISTS in _SCHEMA_SQL.
        # Stamp v2 migration.
        conn.execute(
            "INSERT OR IGNORE INTO schema_version (version, applied_at) VALUES (?, ?)",
            (2, datetime.now(timezone.utc).isoformat()),
        )
        logger.info("Migration v1 → v2 complete.")

    # ── File operations ───────────────────────────────────────────────

    def upsert_file(
        self,
        path: str,
        original_name: str,
        extension: str,
        size: int,
        sha256: str | None = None,
        normalized_name: str | None = None,
        status: str = "scanned",
        notes: str | None = None,
    ) -> int:
        """Insert or update a file record.

        Args:
            path: Absolute path to the file.
            original_name: The original filename.
            extension: File extension (lowercase, with dot).
            size: File size in bytes.
            sha256: SHA-256 hash of the file contents.
            normalized_name: Normalized version of the filename.
            status: Current processing status.
            notes: Optional free-text notes.

        Returns:
            The row ID of the inserted/updated file.
        """
        scan_ts = datetime.now(timezone.utc).isoformat()
        cursor = self.conn.execute(
            """
            INSERT INTO files (path, original_name, normalized_name, extension, size, sha256, scan_timestamp, status, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                original_name=excluded.original_name,
                normalized_name=COALESCE(excluded.normalized_name, files.normalized_name),
                extension=excluded.extension,
                size=excluded.size,
                sha256=COALESCE(excluded.sha256, files.sha256),
                scan_timestamp=excluded.scan_timestamp,
                status=excluded.status,
                notes=COALESCE(excluded.notes, files.notes)
            """,
            (path, original_name, normalized_name, extension, size, sha256, scan_ts, status, notes),
        )
        self._commit_or_defer()
        return cursor.lastrowid  # type: ignore[return-value]

    def update_file_hash(self, file_id: int, sha256: str) -> None:
        """Set the SHA-256 hash for a file and insert into the hashes table.

        Args:
            file_id: The file's row ID.
            sha256: The computed SHA-256 hex digest.
        """
        self.conn.execute(
            "UPDATE files SET sha256 = ? WHERE id = ?", (sha256, file_id)
        )
        self.conn.execute(
            "INSERT OR IGNORE INTO hashes (sha256, file_id) VALUES (?, ?)",
            (sha256, file_id),
        )
        self._commit_or_defer()

    def update_file_normalized_name(self, file_id: int, normalized_name: str) -> None:
        """Set the normalized filename for a file.

        Args:
            file_id: The file's row ID.
            normalized_name: The normalized filename.
        """
        self.conn.execute(
            "UPDATE files SET normalized_name = ?, status = 'normalized' WHERE id = ?",
            (normalized_name, file_id),
        )
        self._commit_or_defer()

    def update_file_status(self, file_id: int, status: str, notes: str | None = None) -> None:
        """Update the processing status of a file.

        Args:
            file_id: The file's row ID.
            status: New status string.
            notes: Optional notes to append.
        """
        if notes:
            self.conn.execute(
                "UPDATE files SET status = ?, notes = COALESCE(notes || '; ' || ?, ?) WHERE id = ?",
                (status, notes, notes, file_id),
            )
        else:
            self.conn.execute(
                "UPDATE files SET status = ? WHERE id = ?", (status, file_id)
            )
        self._commit_or_defer()

    # ── Duplicate group operations ────────────────────────────────────

    def create_duplicate_group(
        self, group_type: str, sha256: str | None = None
    ) -> int:
        """Create a new duplicate group.

        Args:
            group_type: 'exact' or 'possible'.
            sha256: The hash for exact duplicates, None for possible.

        Returns:
            The row ID of the new group.
        """
        cursor = self.conn.execute(
            "INSERT INTO duplicate_groups (group_type, sha256, created_at) VALUES (?, ?, ?)",
            (group_type, sha256, datetime.now(timezone.utc).isoformat()),
        )
        self._commit_or_defer()
        return cursor.lastrowid  # type: ignore[return-value]

    def add_file_to_duplicate_group(
        self, group_id: int, file_id: int, is_canonical: bool = False
    ) -> None:
        """Associate a file with a duplicate group.

        Args:
            group_id: The duplicate group's row ID.
            file_id: The file's row ID.
            is_canonical: Whether this file is the "keeper" in the group.
        """
        self.conn.execute(
            "INSERT OR IGNORE INTO duplicate_group_members (group_id, file_id, is_canonical) VALUES (?, ?, ?)",
            (group_id, file_id, int(is_canonical)),
        )
        self._commit_or_defer()

    # ── Proposed action operations ────────────────────────────────────

    def add_proposed_action(
        self,
        file_id: int,
        action_type: str,
        source_path: str,
        proposed_path: str,
        reason: str | None = None,
    ) -> int:
        """Record a proposed file action.

        Args:
            file_id: The file's row ID.
            action_type: 'rename', 'move', or 'quarantine'.
            source_path: Current file path.
            proposed_path: Target file path.
            reason: Why this action is proposed.

        Returns:
            The row ID of the proposed action.
        """
        cursor = self.conn.execute(
            """INSERT INTO proposed_actions
               (file_id, action_type, source_path, proposed_path, reason)
               VALUES (?, ?, ?, ?, ?)""",
            (file_id, action_type, source_path, proposed_path, reason),
        )
        self._commit_or_defer()
        return cursor.lastrowid  # type: ignore[return-value]

    def mark_action_applied(self, action_id: int) -> None:
        """Mark a proposed action as applied.

        Args:
            action_id: The proposed action's row ID.
        """
        self.conn.execute(
            "UPDATE proposed_actions SET applied = 1, applied_at = ? WHERE id = ?",
            (datetime.now(timezone.utc).isoformat(), action_id),
        )
        self._commit_or_defer()

    def mark_action_rolled_back(self, action_id: int) -> None:
        """Mark a proposed action as rolled back.

        Args:
            action_id: The proposed action's row ID.
        """
        self.conn.execute(
            "UPDATE proposed_actions SET rolled_back = 1, rolled_back_at = ? WHERE id = ?",
            (datetime.now(timezone.utc).isoformat(), action_id),
        )
        self._commit_or_defer()

    # ── Query helpers ─────────────────────────────────────────────────

    def get_all_files(self) -> list[sqlite3.Row]:
        """Return all file records."""
        return self.conn.execute("SELECT * FROM files ORDER BY path").fetchall()

    def get_files_by_hash(self, sha256: str) -> list[sqlite3.Row]:
        """Return all files with the given SHA-256 hash."""
        return self.conn.execute(
            "SELECT * FROM files WHERE sha256 = ? ORDER BY path", (sha256,)
        ).fetchall()

    def get_exact_duplicate_groups(self) -> list[sqlite3.Row]:
        """Return all exact duplicate groups."""
        return self.conn.execute(
            "SELECT * FROM duplicate_groups WHERE group_type = 'exact'"
        ).fetchall()

    def get_possible_duplicate_groups(self) -> list[sqlite3.Row]:
        """Return all possible duplicate groups."""
        return self.conn.execute(
            "SELECT * FROM duplicate_groups WHERE group_type = 'possible'"
        ).fetchall()

    def get_group_members(self, group_id: int) -> list[sqlite3.Row]:
        """Return all files in a duplicate group."""
        return self.conn.execute(
            """SELECT f.* FROM files f
               JOIN duplicate_group_members dgm ON f.id = dgm.file_id
               WHERE dgm.group_id = ?""",
            (group_id,),
        ).fetchall()

    def get_pending_actions(self) -> list[sqlite3.Row]:
        """Return all proposed actions that haven't been applied yet."""
        return self.conn.execute(
            "SELECT * FROM proposed_actions WHERE applied = 0 ORDER BY id"
        ).fetchall()

    def get_applied_actions(self) -> list[sqlite3.Row]:
        """Return all applied actions (for rollback)."""
        return self.conn.execute(
            "SELECT * FROM proposed_actions WHERE applied = 1 AND rolled_back = 0 ORDER BY id"
        ).fetchall()

    def get_file_by_id(self, file_id: int) -> sqlite3.Row | None:
        """Return a single file by ID."""
        return self.conn.execute(
            "SELECT * FROM files WHERE id = ?", (file_id,)
        ).fetchone()

    def get_file_by_path(self, path: str) -> sqlite3.Row | None:
        """Return a single file by path."""
        return self.conn.execute(
            "SELECT * FROM files WHERE path = ?", (path,)
        ).fetchone()

    def get_unusual_name_files(self) -> list[sqlite3.Row]:
        """Return files with unusual characteristics in their names.

        'Unusual' means: very long names (> 120 chars), or names with
        characters outside the common ROM naming set. The normalizer's
        is_unusual_name() function does a more precise check.
        """
        return self.conn.execute(
            """SELECT * FROM files
               WHERE LENGTH(original_name) > 120
               OR original_name GLOB '*[^a-zA-Z0-9 ._()\\-\\[\\]!+]*'
               ORDER BY path"""
        ).fetchall()

    def get_member_canonical_status(self, group_id: int, file_id: int) -> bool:
        """Check whether a file is the canonical member of a duplicate group.

        Args:
            group_id: The duplicate group's row ID.
            file_id: The file's row ID.

        Returns:
            True if the file is marked as canonical in the group.
        """
        row = self.conn.execute(
            "SELECT is_canonical FROM duplicate_group_members WHERE group_id = ? AND file_id = ?",
            (group_id, file_id),
        ).fetchone()
        return bool(row["is_canonical"]) if row else False

    def update_proposed_action_source_path(self, action_id: int, new_source_path: str) -> None:
        """Update the source path of a pending proposed action.

        Used after a rename to keep subsequent move actions in sync.

        Args:
            action_id: The proposed action's row ID.
            new_source_path: The updated source path.
        """
        self.conn.execute(
            "UPDATE proposed_actions SET source_path = ? WHERE id = ? AND applied = 0",
            (new_source_path, action_id),
        )
        self._commit_or_defer()

    def get_stats(self) -> dict[str, int]:
        """Return summary statistics."""
        total = self.conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        unique_hashes = self.conn.execute(
            "SELECT COUNT(DISTINCT sha256) FROM files WHERE sha256 IS NOT NULL"
        ).fetchone()[0]
        exact_groups = self.conn.execute(
            "SELECT COUNT(*) FROM duplicate_groups WHERE group_type = 'exact'"
        ).fetchone()[0]
        possible_groups = self.conn.execute(
            "SELECT COUNT(*) FROM duplicate_groups WHERE group_type = 'possible'"
        ).fetchone()[0]
        pending_actions = self.conn.execute(
            "SELECT COUNT(*) FROM proposed_actions WHERE applied = 0"
        ).fetchone()[0]
        identified = self.conn.execute(
            "SELECT COUNT(*) FROM files WHERE dat_game_name IS NOT NULL"
        ).fetchone()[0]
        archives = self.conn.execute(
            "SELECT COUNT(*) FROM files WHERE is_archive = 1"
        ).fetchone()[0]
        return {
            "total_files": total,
            "unique_hashes": unique_hashes,
            "exact_duplicate_groups": exact_groups,
            "possible_duplicate_groups": possible_groups,
            "pending_actions": pending_actions,
            "identified_files": identified,
            "archive_files": archives,
        }

    # ── Archive entry operations ──────────────────────────────────────

    def add_archive_entry(
        self,
        file_id: int,
        entry_name: str,
        entry_size: int,
        compressed_size: int = 0,
        crc32: str | None = None,
        sha256: str | None = None,
    ) -> int:
        """Record a single entry inside an archive.

        Args:
            file_id: The parent archive file's row ID.
            entry_name: Filename inside the archive.
            entry_size: Uncompressed size in bytes.
            compressed_size: Compressed size in bytes.
            crc32: CRC32 hash of the entry.
            sha256: SHA-256 hash of the entry.

        Returns:
            The row ID of the archive entry.
        """
        cursor = self.conn.execute(
            """INSERT INTO archive_entries
               (file_id, entry_name, entry_size, compressed_size, crc32, sha256)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (file_id, entry_name, entry_size, compressed_size, crc32, sha256),
        )
        self._commit_or_defer()
        return cursor.lastrowid  # type: ignore[return-value]

    def get_archive_entries(self, file_id: int) -> list[sqlite3.Row]:
        """Return all entries inside an archive file."""
        return self.conn.execute(
            "SELECT * FROM archive_entries WHERE file_id = ? ORDER BY entry_name",
            (file_id,),
        ).fetchall()

    def mark_file_as_archive(self, file_id: int, fingerprint: str | None = None) -> None:
        """Mark a file as an archive and optionally set its content fingerprint."""
        self.conn.execute(
            "UPDATE files SET is_archive = 1, archive_fingerprint = ? WHERE id = ?",
            (fingerprint, file_id),
        )
        self._commit_or_defer()

    def update_file_dat_info(
        self,
        file_id: int,
        game_name: str | None = None,
        description: str | None = None,
        system: str | None = None,
    ) -> None:
        """Update a file's DAT identification info."""
        self.conn.execute(
            """UPDATE files SET
               dat_game_name = COALESCE(?, dat_game_name),
               dat_description = COALESCE(?, dat_description),
               dat_system = COALESCE(?, dat_system)
               WHERE id = ?""",
            (game_name, description, system, file_id),
        )
        self._commit_or_defer()

    def update_file_hashes(
        self,
        file_id: int,
        md5: str | None = None,
        crc32: str | None = None,
    ) -> None:
        """Update a file's MD5 and/or CRC32 hashes."""
        if md5:
            self.conn.execute(
                "UPDATE files SET md5 = ? WHERE id = ?", (md5, file_id)
            )
        if crc32:
            self.conn.execute(
                "UPDATE files SET crc32 = ? WHERE id = ?", (crc32, file_id)
            )
        self._commit_or_defer()

    # ── DAT file operations ────────────────────────────────────────────

    def upsert_dat_file(
        self,
        filename: str,
        name: str | None = None,
        description: str | None = None,
        category: str | None = None,
        version: str | None = None,
    ) -> int:
        """Insert or update a DAT file record.

        Returns:
            The row ID of the DAT file.
        """
        cursor = self.conn.execute(
            """INSERT INTO dat_files (filename, name, description, category, version, loaded_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(filename) DO UPDATE SET
                   name=excluded.name, description=excluded.description,
                   category=excluded.category, version=excluded.version,
                   loaded_at=excluded.loaded_at""",
            (filename, name, description, category, version,
             datetime.now(timezone.utc).isoformat()),
        )
        self._commit_or_defer()
        return cursor.lastrowid  # type: ignore[return-value]

    def add_dat_game(
        self,
        dat_id: int,
        game_name: str,
        description: str = "",
        category: str = "",
        clone_of: str | None = None,
        year: str | None = None,
        manufacturer: str | None = None,
    ) -> int:
        """Insert a DAT game entry."""
        cursor = self.conn.execute(
            """INSERT INTO dat_games
               (dat_id, game_name, description, category, clone_of, year, manufacturer)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (dat_id, game_name, description, category, clone_of, year, manufacturer),
        )
        self._commit_or_defer()
        return cursor.lastrowid  # type: ignore[return-value]

    def add_dat_rom(
        self,
        game_id: int,
        rom_name: str,
        size: int,
        crc32: str | None = None,
        md5: str | None = None,
        sha256: str | None = None,
        merge_name: str | None = None,
    ) -> int:
        """Insert a DAT ROM entry."""
        cursor = self.conn.execute(
            """INSERT INTO dat_roms
               (game_id, rom_name, size, crc32, md5, sha256, merge_name)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (game_id, rom_name, size, crc32, md5, sha256, merge_name),
        )
        self._commit_or_defer()
        return cursor.lastrowid  # type: ignore[return-value]

    def lookup_dat_by_sha256(self, sha256: str) -> list[sqlite3.Row]:
        """Find DAT ROM entries matching a SHA-256 hash."""
        return self.conn.execute(
            """SELECT dr.*, dg.game_name, dg.description, dg.category, dg.clone_of,
                      df.filename as dat_filename
               FROM dat_roms dr
               JOIN dat_games dg ON dr.game_id = dg.id
               JOIN dat_files df ON dg.dat_id = df.id
               WHERE dr.sha256 = ?""",
            (sha256,),
        ).fetchall()

    def lookup_dat_by_md5(self, md5: str) -> list[sqlite3.Row]:
        """Find DAT ROM entries matching an MD5 hash."""
        return self.conn.execute(
            """SELECT dr.*, dg.game_name, dg.description, dg.category, dg.clone_of,
                      df.filename as dat_filename
               FROM dat_roms dr
               JOIN dat_games dg ON dr.game_id = dg.id
               JOIN dat_files df ON dg.dat_id = df.id
               WHERE dr.md5 = ?""",
            (md5,),
        ).fetchall()

    def lookup_dat_by_crc32(self, crc32: str) -> list[sqlite3.Row]:
        """Find DAT ROM entries matching a CRC32 hash."""
        return self.conn.execute(
            """SELECT dr.*, dg.game_name, dg.description, dg.category, dg.clone_of,
                      df.filename as dat_filename
               FROM dat_roms dr
               JOIN dat_games dg ON dr.game_id = dg.id
               JOIN dat_files df ON dg.dat_id = df.id
               WHERE dr.crc32 = ?""",
            (crc32,),
        ).fetchall()

    # ── Selective rollback ─────────────────────────────────────────────

    def get_applied_actions_range(
        self, last_n: int | None = None, action_id: int | None = None
    ) -> list[sqlite3.Row]:
        """Return applied actions for selective rollback.

        Args:
            last_n: If set, return only the last N applied actions.
            action_id: If set, return only the specific action.

        Returns:
            List of applied action rows.
        """
        if action_id is not None:
            return self.conn.execute(
                "SELECT * FROM proposed_actions WHERE id = ? AND applied = 1 AND rolled_back = 0",
                (action_id,),
            ).fetchall()
        elif last_n is not None:
            return self.conn.execute(
                """SELECT * FROM proposed_actions
                   WHERE applied = 1 AND rolled_back = 0
                   ORDER BY id DESC LIMIT ?""",
                (last_n,),
            ).fetchall()
        else:
            return self.get_applied_actions()

    # ── Database backup ────────────────────────────────────────────────

    def backup(self, backup_path: Path | None = None) -> Path:
        """Create a backup copy of the database file.

        Args:
            backup_path: Optional target path. Defaults to <db_path>.bak.

        Returns:
            Path to the backup file.
        """
        import shutil

        if backup_path is None:
            backup_path = Path(str(self.db_path) + ".bak")

        # Close connection to ensure all data is flushed.
        if self._conn is not None:
            self._conn.close()
            self._conn = None

        shutil.copy2(str(self.db_path), str(backup_path))
        logger.info("Database backed up to %s", backup_path)
        return backup_path


class _Transaction:
    """Context manager for batching database operations into a single commit.

    Usage:
        with db.transaction():
            db.upsert_file(...)
            db.update_file_hash(...)
            # Single commit at exit instead of per-operation commits.
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    def __enter__(self) -> "_Transaction":
        self._db._in_transaction = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            if exc_type is None:
                self._db.conn.commit()
            else:
                self._db.conn.rollback()
        finally:
            self._db._in_transaction = False