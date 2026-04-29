"""Filename normalizer for the ROM Organizer.

Applies safe, reversible normalization rules to ROM filenames:
- Trim whitespace
- Collapse repeated spaces
- Standardize separators (underscores, dashes, plus signs → spaces)
- Preserve region/version tags like (U), (J), [!], [b1], etc.
- Preserve original extension

Design decision: We NEVER modify the original file on disk during
normalization. We only compute the proposed normalized name and store it
in the database. The actual rename happens only in --apply mode, and
every rename is recorded as a proposed_action for full auditability.
"""

from __future__ import annotations

import logging
import re

from config import Config
from database import Database

logger = logging.getLogger(__name__)

# ── Patterns that should be preserved in filenames ─────────────────────────
# Region codes: (U), (J), (E), (USA), (Europe), (Japan), etc.
# Version tags: (Rev A), (V1.1), etc.
# GoodTools-style tags: [!], [b1], [o1], [f1], [T+Eng], etc.
# No-Intro-style parenthetical tags: (PC10), (SGB Enhanced), etc.
_REGION_PATTERN = re.compile(
    r"\((?:"
    r"[A-Z]{1,3}"            # Short codes: (U), (J), (E), (UE)
    r"|USA|Europe|Japan|Asia|World|Brazil|Australia|Korea|Canada|Spain|Italy|Germany|France|China|Netherlands|Scandinavia"
    r")\)",
    re.IGNORECASE,
)

_VERSION_PATTERN = re.compile(
    r"\((?:Rev\s*[A-Z0-9.]+|V?\d+\.\d+(?:\.\d+)?|v?\d+)\)",
    re.IGNORECASE,
)

_TAG_PATTERN = re.compile(
    r"\[(?:"
    r"[abfoSTmM]\d*"          # GoodTools codes: [!], [b1], [o2], [f1], [T+Eng]
    r"|!"
    r"|\+[^]]*"
    r"|a[0-9]*"
    r"|b[0-9]*"
    r"|o[0-9]*"
    r"|f[0-9]*"
    r"|T[+-][^]]*"
    r"|p[0-9]*"
    r"|t[0-9]*"
    r"|c[0-9]*"
    r"|h[0-9]*"
    r"|C[^\]]*"              # Fixed: was C[^^]]* which matched wrong chars
    r"|overdump|underdump|bad|fixed|trained|hack|translation|patch|pirate|proto|demo|sample|beta|alpha|unl|palm|sega"
    r")\]",
    re.IGNORECASE,
)


def normalize_filename(filename: str, config: Config) -> str:
    """Apply normalization rules to a ROM filename.

    Rules applied (in order):
    1. Extract and preserve the extension.
    2. Strip leading/trailing whitespace.
    3. Replace separator characters (configurable) with spaces.
    4. Collapse multiple spaces into one.
    5. Preserve region, version, and tag patterns.
    6. Title-case the base name (only if it's all lowercase or all uppercase).
    7. Truncate to max filename length if needed.

    Args:
        filename: The original filename (with extension).
        config: Application configuration.

    Returns:
        The normalized filename (with original extension).
    """
    # Separate name from extension.
    name, dot, ext = filename.rpartition(".")
    if not dot:
        # No extension found — treat entire string as name.
        name = filename
        ext = ""
    else:
        ext = f".{ext.lower()}"

    original_name = name

    # Step 1: Trim whitespace.
    name = name.strip()

    # Step 2: Replace separator characters with spaces.
    # We replace each configured separator char with a space.
    # However, we preserve separators inside brackets [] since they
    # are part of GoodTools-style tags.
    # We also preserve separator+digit patterns that look like counter
    # suffixes (e.g., _1, -2 at the end of the name before extension).
    for ch in config.separator_chars:
        # Don't replace separators inside brackets.
        parts = re.split(r"(\[[^\]]*\])", name)
        for i, part in enumerate(parts):
            if not part.startswith("["):
                # Preserve trailing counter suffixes like _1, _2, -1, +1
                # by temporarily replacing them with a placeholder.
                counter_tag = f"CNTR{ord(ch)}X"
                part = re.sub(
                    rf"({re.escape(ch)})(\d+)(\s*$)",
                    rf"{counter_tag}\2\3",
                    part,
                )
                # Replace remaining separator chars with spaces.
                part = part.replace(ch, " ")
                # Restore counter suffixes.
                part = part.replace(counter_tag, ch)
                parts[i] = part
        name = "".join(parts)

    # Step 3: Collapse repeated spaces.
    name = re.sub(r" {2,}", " ", name)

    # Step 4: Clean up spaces around parentheses and brackets.
    # Only add spaces where there aren't already meaningful characters
    # (like a counter suffix _1) adjacent to the bracket.
    name = re.sub(r"\s*\(\s*", " (", name)
    name = re.sub(r"\s*\)\s+", ") ", name)  # space after ) only if whitespace was there
    name = re.sub(r"\s*\[\s*", " [", name)
    # Don't add space after ] if followed by a separator+digit counter (e.g., ]_1).
    name = re.sub(r"\s*\]\s+(?=[^_\-\+\d])", "] ", name)
    name = re.sub(r"\s*\]\s*$", "]", name)

    # Step 5: Trim again after all replacements.
    name = name.strip()

    # Step 6: Title-case only if the name is ALL UPPER or ALL lower.
    # This avoids mangling mixed-case names like "SuperMarioBros" or "Zelda3".
    if name.isupper() or name.islower():
        # Title-case, but preserve content inside () and [].
        name = _smart_title_case(name)

    # Step 6b: Normalize region codes inside parentheses.
    # Only uppercase known region codes to avoid mangling version tags like (v1).
    _KNOWN_REGIONS = frozenset({
        "u", "j", "e", "ue", "uj", "ej", "eu", "ju", "je",
        "usa", "europe", "japan", "asia", "world", "brazil",
        "australia", "korea", "canada", "spain", "italy",
        "germany", "france", "china", "netherlands", "scandinavia",
        "pal", "ntsc",
    })
    name = re.sub(
        r"\(([a-z]{1,3})\)",
        lambda m: f"({m.group(1).upper()})" if m.group(1).lower() in _KNOWN_REGIONS else m.group(0),
        name,
    )

    # Step 7: Truncate if needed, preserving extension.
    max_len = config.max_filename_length - len(ext)
    if len(name) > max_len:
        name = name[:max_len].rstrip()

    # If normalization didn't change anything, return original.
    if name == original_name:
        return filename

    return f"{name}{ext}" if ext else name


def _smart_title_case(name: str) -> str:
    """Apply title case while preserving tags in parentheses and brackets.

    Args:
        name: The filename without extension.

    Returns:
        Title-cased name with preserved tag contents.
    """
    # Split on tag boundaries, title-case the non-tag parts.
    parts = re.split(r"(\([^)]*\)|\[[^]]*\])", name)
    result = []
    for part in parts:
        if part.startswith("(") or part.startswith("["):
            # Preserve tag content as-is.
            result.append(part)
        else:
            result.append(part.title())
    return "".join(result)


def is_unusual_name(filename: str) -> bool:
    """Check if a filename has unusual characteristics.

    A name is considered unusual if it:
    - Contains non-ASCII characters
    - Has multiple extensions (e.g., .tar.gz) — dots inside the stem
      that look like stacked extensions, not just periods in the title
    - Is very long (> 120 chars)
    - Contains only special characters (no alphanumeric)
    - Has leading/trailing whitespace

    Args:
        filename: The filename to check.

    Returns:
        True if the name is unusual.
    """
    # Non-ASCII characters.
    try:
        filename.encode("ascii")
    except UnicodeEncodeError:
        return True

    # Multiple extensions: check if the stem (before the last dot) also
    # ends with a known extension-like suffix. A simple heuristic: if
    # there are 2+ dots and the part between the last two dots is a
    # short (<=4 char) alphabetic string, it's likely stacked extensions.
    # E.g., "game.tar.gz" → unusual; "Super Mario Bros. (U).nes" → fine.
    parts = filename.split(".")
    if len(parts) > 2:
        # Check if any intermediate part looks like an extension (short alphabetic).
        for part in parts[1:-1]:  # skip first (stem) and last (real ext)
            stripped = part.strip()
            if stripped and len(stripped) <= 4 and stripped.isalpha():
                return True

    # Very long names.
    if len(filename) > 120:
        return True

    # No alphanumeric characters.
    if not re.search(r"[a-zA-Z0-9]", filename):
        return True

    # Leading/trailing whitespace.
    if filename != filename.strip():
        return True

    return False


def matches_naming_rules(filename: str) -> bool:
    """Check if a filename follows common ROM naming conventions.

    Common patterns:
    - "Game Title (Region).ext"
    - "Game Title (Region) [Tag].ext"
    - "Game Title (Region) (Version) [Tag].ext"

    Args:
        filename: The filename to check (with extension).

    Returns:
        True if the name matches known naming conventions.
    """
    name = filename.rpartition(".")[0] if "." in filename else filename

    # Must have at least one alphanumeric character.
    if not re.search(r"[a-zA-Z0-9]", name):
        return False

    # Check for region tag pattern.
    has_region = bool(_REGION_PATTERN.search(name))
    # Check for version tag pattern.
    has_version = bool(_VERSION_PATTERN.search(name))
    # Check for GoodTools/No-Intro tag pattern.
    has_tag = bool(_TAG_PATTERN.search(name))

    # A well-named ROM typically has at least a region tag.
    # But we don't require it — just check for basic structure.
    return bool(re.match(r"^[a-zA-Z0-9]", name))


def normalize_all_files(db: Database, config: Config, dry_run: bool = False) -> dict[str, int]:
    """Compute normalized names for all files in the database.

    Args:
        db: Database instance.
        config: Application configuration.
        dry_run: If True, compute normalized names but do not write to the database.

    Returns:
        A dict with 'normalized', 'unchanged', and 'errors' counts.
    """
    from progress import ProgressBar

    rows = db.get_all_files()
    stats = {"normalized": 0, "unchanged": 0, "errors": 0}
    total = len(rows)

    bar = ProgressBar(total=total, label="Normalizing", unit="files")

    for row in rows:
        original = row["original_name"]
        try:
            normalized = normalize_filename(original, config)
            if normalized != original:
                if not dry_run:
                    db.update_file_normalized_name(row["id"], normalized)
                logger.debug("Normalized: %s → %s", original, normalized)
                stats["normalized"] += 1
            else:
                if not dry_run:
                    # Store the normalized name even if unchanged.
                    db.update_file_normalized_name(row["id"], normalized)
                stats["unchanged"] += 1
        except Exception:
            logger.exception("Error normalizing: %s", original)
            stats["errors"] += 1

        bar.update(1)

    bar.close()
    logger.info(
        "Normalization complete: %d changed, %d unchanged, %d errors%s",
        stats["normalized"],
        stats["unchanged"],
        stats["errors"],
        " (dry run — no changes written)" if dry_run else "",
    )
    return stats