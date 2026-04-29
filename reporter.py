"""Report generation for the ROM Organizer.

Produces a human-readable summary report of the scan results, including:
- Total files scanned
- Unique hashes
- Exact duplicate groups
- Possible duplicate groups
- Files with unusual names
- Files that don't match naming conventions
- Proposed actions summary

Design decision: Reports are generated from the database so they can be
run at any time, not just immediately after a scan. This supports the
auditability requirement.
"""

from __future__ import annotations

import logging
from pathlib import Path

from database import Database
from normalizer import is_unusual_name, matches_naming_rules

logger = logging.getLogger(__name__)


def generate_report(db: Database, output_path: Path | None = None) -> str:
    """Generate a comprehensive report from the database.

    Args:
        db: Database instance.
        output_path: Optional path to write the report to a file.

    Returns:
        The report as a string.
    """
    lines: list[str] = []
    lines.append("=" * 72)
    lines.append("  ROM ORGANIZER — SCAN REPORT")
    lines.append("=" * 72)
    lines.append("")

    # ── Summary statistics ─────────────────────────────────────────────
    stats = db.get_stats()
    lines.append("── SUMMARY ──────────────────────────────────────────────────────")
    lines.append(f"  Total ROM files scanned:       {stats['total_files']}")
    lines.append(f"  Unique SHA-256 hashes:         {stats['unique_hashes']}")
    lines.append(f"  Exact duplicate groups:        {stats['exact_duplicate_groups']}")
    lines.append(f"  Possible duplicate groups:     {stats['possible_duplicate_groups']}")
    lines.append(f"  Pending proposed actions:      {stats['pending_actions']}")
    lines.append(f"  Identified files (DAT match):  {stats['identified_files']}")
    lines.append(f"  Archive files inspected:       {stats['archive_files']}")
    if stats.get("scan_errors", 0) or stats.get("hash_errors", 0):
        lines.append("")
        lines.append(f"  ⚠ Scan errors:                {stats.get('scan_errors', 0)}")
        lines.append(f"  ⚠ Hash errors:                {stats.get('hash_errors', 0)}")
    lines.append("")

    # ── Exact duplicate groups ──────────────────────────────────────────
    exact_groups = db.get_exact_duplicate_groups()
    if exact_groups:
        lines.append("── EXACT DUPLICATES (same SHA-256) ─────────────────────────────")
        for group in exact_groups:
            members = db.get_group_members(group["id"])
            lines.append(f"  Group {group['id']} — Hash: {group['sha256'][:16]}...")
            for member in members:
                is_canonical = db.get_member_canonical_status(group["id"], member["id"])
                marker = "★" if is_canonical else " "
                lines.append(f"    {marker} {member['path']} ({member['size']:,} bytes)")
            lines.append("")

    # ── Possible duplicate groups ───────────────────────────────────────
    possible_groups = db.get_possible_duplicate_groups()
    if possible_groups:
        lines.append("── POSSIBLE DUPLICATES (similar name + same size) ──────────────")
        for group in possible_groups:
            members = db.get_group_members(group["id"])
            lines.append(f"  Group {group['id']}:")
            for member in members:
                is_canonical = db.get_member_canonical_status(group["id"], member["id"])
                marker = "★" if is_canonical else " "
                lines.append(
                    f"    {marker} {member['path']} "
                    f"(hash: {member['sha256'][:16] if member['sha256'] else 'N/A'}..., "
                    f"{member['size']:,} bytes)"
                )
            lines.append("")

    # ── Unusual filenames ───────────────────────────────────────────────
    unusual_files = db.get_unusual_name_files()
    if unusual_files:
        lines.append("── UNUSUAL FILENAMES ───────────────────────────────────────────")
        for f in unusual_files:
            reasons = []
            if is_unusual_name(f["original_name"]):
                reasons.append("unusual pattern")
            if len(f["original_name"]) > 120:
                reasons.append("very long name")
            if f["original_name"].count(".") > 1:
                reasons.append("multiple extensions")
            lines.append(f"  • {f['path']} [{', '.join(reasons)}]")
        lines.append("")

    # ── Files not matching naming conventions ────────────────────────────
    all_files = db.get_all_files()
    non_matching = [f for f in all_files if not matches_naming_rules(f["original_name"])]
    if non_matching:
        lines.append("── FILES NOT MATCHING NAMING CONVENTIONS ───────────────────────")
        for f in non_matching:
            lines.append(f"  • {f['path']}")
        lines.append("")

    # ── Error / corrupted files ─────────────────────────────────────────
    error_files = db.get_error_files()
    if error_files:
        lines.append("── ERROR / CORRUPTED FILES ─────────────────────────────────────")
        for f in error_files:
            status_tag = "SCAN" if f["status"] == "scan_error" else "HASH"
            notes = f" — {f['notes']}" if f["notes"] else ""
            lines.append(f"  ⚠ [{status_tag}] {f['path']}{notes}")
        lines.append("")

    # ── Proposed actions summary ────────────────────────────────────────
    pending = db.get_pending_actions()
    if pending:
        renames = sum(1 for a in pending if a["action_type"] == "rename")
        quarantines = sum(1 for a in pending if a["action_type"] == "quarantine")
        moves = sum(1 for a in pending if a["action_type"] == "move")

        lines.append("── PROPOSED ACTIONS ─────────────────────────────────────────────")
        lines.append(f"  Renames:     {renames}")
        lines.append(f"  Quarantines: {quarantines}")
        lines.append(f"  Moves:       {moves}")
        lines.append("")

        lines.append("── PROPOSED ACTION DETAILS ─────────────────────────────────────")
        for action in pending:
            lines.append(
                f"  [{action['action_type'].upper()}] "
                f"{action['source_path']} → {action['proposed_path']}"
            )
            if action["reason"]:
                lines.append(f"    Reason: {action['reason']}")
        lines.append("")

    lines.append("=" * 72)
    lines.append("  END OF REPORT")
    lines.append("=" * 72)

    report = "\n".join(lines)

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report, encoding="utf-8")
        logger.info("Report written to %s", output_path)

    return report