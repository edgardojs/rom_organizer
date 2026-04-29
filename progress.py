"""Progress reporting for the ROM Organizer.

Provides a lightweight progress bar and folder size estimation.
Uses only stdlib — no third-party dependencies like tqdm.

Design decision: We implement our own progress bar rather than depending
on tqdm or similar. The bar renders to stderr so it doesn't interfere
with stdout piping. It auto-detects terminal width and degrades
gracefully when stderr is not a TTY (logs periodic updates instead).
"""

from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


def _is_terminal() -> bool:
    """Check if stderr is connected to a terminal."""
    return hasattr(sys.stderr, "isatty") and sys.stderr.isatty()


def _terminal_width() -> int:
    """Get terminal width, defaulting to 80 if not detectable."""
    try:
        return os.get_terminal_size(sys.stderr.fileno()).columns
    except (OSError, ValueError):
        return 80


def _format_size(nbytes: int) -> str:
    """Format a byte count as a human-readable string.

    Args:
        nbytes: Number of bytes.

    Returns:
        Human-readable string like "1.2 GiB".
    """
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(nbytes) < 1024.0:
            return f"{nbytes:.1f} {unit}" if unit != "B" else f"{nbytes} {unit}"
        nbytes /= 1024.0
    return f"{nbytes:.1f} PiB"


def _format_duration(seconds: float) -> str:
    """Format a duration in seconds as a human-readable string.

    Args:
        seconds: Duration in seconds.

    Returns:
        String like "2m 15s" or "45s".
    """
    if seconds < 0:
        return "--"
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes, secs = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m"


@dataclass
class FolderEstimate:
    """Result of estimating a folder's ROM content size."""

    total_files: int = 0
    total_bytes: int = 0
    skipped_files: int = 0
    elapsed: float = 0.0

    @property
    def human_size(self) -> str:
        return _format_size(self.total_bytes)

    def __str__(self) -> str:
        parts = [f"{self.total_files:,} files"]
        if self.skipped_files:
            parts.append(f"{self.skipped_files:,} filtered out")
        parts.append(f"{self.human_size}")
        return ", ".join(parts)


def estimate_folder(
    root: Path,
    extensions: set[str],
    exclude_dirs: set[str],
    min_file_size: int = 1024,
) -> FolderEstimate:
    """Estimate the number and total size of ROM files in a directory.

    This is a fast pre-scan that walks the directory tree to count matching
    files and sum their sizes. It does NOT hash or inspect archives — it
    just gives the user a sense of scale before the real work begins.

    Args:
        root: Root directory to estimate.
        extensions: Set of lowercase file extensions to include.
        exclude_dirs: Set of directory names to skip.
        min_file_size: Minimum file size in bytes.

    Returns:
        A FolderEstimate with counts and sizes.
    """
    start = time.monotonic()
    est = FolderEstimate()

    for path in root.rglob("*"):
        if any(part in exclude_dirs for part in path.parts):
            continue
        if not path.is_file():
            continue
        if path.suffix.lower() not in extensions:
            continue
        if path.name.startswith("."):
            continue
        try:
            size = path.stat().st_size
        except OSError:
            continue
        if size < min_file_size:
            est.skipped_files += 1
            continue
        est.total_files += 1
        est.total_bytes += size

    est.elapsed = time.monotonic() - start
    return est


class ProgressBar:
    """A lightweight progress bar that renders to stderr.

    Usage:
        bar = ProgressBar(total=1000, label="Hashing")
        for i in range(1000):
            bar.update(1)
        bar.close()

    When stderr is not a TTY, it falls back to periodic log messages
    instead of rendering a bar.
    """

    def __init__(
        self,
        total: int,
        label: str = "",
        unit: str = "files",
        update_interval: float = 0.25,
        log_interval: int = 100,
    ) -> None:
        """Initialize the progress bar.

        Args:
            total: Total number of items to process.
            label: Short label for the operation (e.g., "Scanning", "Hashing").
            unit: Unit name for display (e.g., "files", "entries").
            update_interval: Minimum seconds between terminal redraws.
            log_interval: Number of items between log messages (non-TTY mode).
        """
        self.total = total
        self.label = label
        self.unit = unit
        self.update_interval = update_interval
        self.log_interval = log_interval

        self._current: int = 0
        self._current_item: str = ""
        self._start_time: float = time.monotonic()
        self._last_draw: float = 0.0
        self._is_tty: bool = _is_terminal()
        self._closed: bool = False

    @property
    def current(self) -> int:
        return self._current

    def update(self, n: int = 1, item: str = "") -> None:
        """Advance the progress bar by n items.

        Args:
            n: Number of items completed.
            item: Optional name of the current item being processed.
        """
        self._current += n
        if item:
            self._current_item = item
        now = time.monotonic()

        if self._is_tty:
            # Throttle redraws to avoid flickering, but always redraw
            # when the item name changes so the user sees what's happening.
            item_changed = item and item != self._current_item
            if (now - self._last_draw) >= self.update_interval or self._current >= self.total or item_changed:
                self._draw()
                self._last_draw = now
        else:
            # Non-TTY: log periodically.
            if self._current % self.log_interval == 0 or self._current >= self.total:
                self._log_progress()

    def set_current(self, value: int) -> None:
        """Set the current progress value directly.

        Args:
            value: The new current count.
        """
        self._current = value
        now = time.monotonic()

        if self._is_tty:
            if (now - self._last_draw) >= self.update_interval or self._current >= self.total:
                self._draw()
                self._last_draw = now
        else:
            if self._current % self.log_interval == 0 or self._current >= self.total:
                self._log_progress()

    def set_current_item(self, name: str) -> None:
        """Set the name of the item currently being processed.

        Forces a redraw on TTY so the user can see what file is being
        worked on, even if the count hasn't changed.

        Args:
            name: Name of the current item (e.g., filename).
        """
        self._current_item = name
        now = time.monotonic()

        if self._is_tty:
            # Always redraw when the item changes so the user sees activity.
            self._draw()
            self._last_draw = now
        else:
            # In non-TTY mode, log the current item at DEBUG level.
            logger.debug("%s: processing %s", self.label, name)

    def close(self) -> None:
        """Finalize the progress bar.

        On a TTY, draws the final state and prints a newline.
        On non-TTY, logs the final summary.
        """
        if self._closed:
            return
        self._closed = True

        if self._is_tty:
            self._draw()
            sys.stderr.write("\n")
            sys.stderr.flush()
        else:
            self._log_progress()

    def _draw(self) -> None:
        """Render the progress bar to stderr."""
        if self._closed and self._current >= self.total:
            pct = 100.0
        else:
            pct = (self._current / self.total * 100) if self.total > 0 else 0.0

        elapsed = time.monotonic() - self._start_time
        if self._current > 0 and self._current < self.total:
            eta = elapsed / self._current * (self.total - self._current)
            eta_str = _format_duration(eta)
        elif self._current >= self.total:
            eta_str = "done"
        else:
            eta_str = "--"

        width = _terminal_width()
        count_str = f"{self._current:,}/{self.total:,}"
        pct_str = f"{pct:5.1f}%"
        elapsed_str = _format_duration(elapsed)

        # Build the status line: label |████████░░░| 45% 450/1000 files 2m ETA
        # Optionally append the current item name on a second line.
        bar_label = self.label
        bar_width = width - len(f" {bar_label} |  {pct_str} {count_str} {self.unit} {elapsed_str} ETA {eta_str} ") - 2
        bar_width = max(bar_width, 10)

        filled = int(bar_width * pct / 100)
        bar = "█" * filled + "░" * (bar_width - filled)

        line1 = f"\r{bar_label} |{bar}| {pct_str} {count_str} {self.unit} {elapsed_str} ETA {eta_str}"
        if len(line1) > width:
            line1 = line1[: width - 1] + "…"

        # Show current item on a second line if we have one.
        if self._current_item:
            # Truncate item name to fit terminal width.
            item_display = self._current_item
            item_prefix = "  → "
            max_item_len = width - len(item_prefix) - 1
            if len(item_display) > max_item_len:
                item_display = "…" + item_display[-(max_item_len - 1):]
            line2 = f"\n  → {item_display}"
        else:
            line2 = ""

        # Move cursor up if we previously drew a second line, so we
        # overwrite both lines cleanly.
        sys.stderr.write(f"\033[2K\r{line1}{line2}")
        sys.stderr.flush()

    def _log_progress(self) -> None:
        """Log progress as a structured message (non-TTY fallback)."""
        pct = (self._current / self.total * 100) if self.total > 0 else 0.0
        elapsed = time.monotonic() - self._start_time
        if self._current > 0 and self._current < self.total:
            eta = elapsed / self._current * (self.total - self._current)
            eta_str = _format_duration(eta)
        elif self._current >= self.total:
            eta_str = "done"
        else:
            eta_str = "--"

        logger.info(
            "%s: %d/%d %s (%.1f%%) elapsed %s ETA %s",
            self.label or "Progress",
            self._current,
            self.total,
            self.unit,
            pct,
            _format_duration(elapsed),
            eta_str,
        )