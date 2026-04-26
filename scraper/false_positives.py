import difflib
from datetime import datetime, timezone
from pathlib import Path

from .db import (
    append_prompt_history,
    delete_false_positive,
    get_false_positives,
    get_prompt_history,
    upsert_false_positive,
)

FP_TYPES = ("extraction", "enrichment")


def load(db_path: Path) -> list[dict]:
    return get_false_positives(db_path)


def add(db_path: Path, name: str, city: str, topic: str, reason: str,
        source_url: str, fp_type: str = "extraction") -> None:
    upsert_false_positive(db_path, name, city, topic, reason, source_url, fp_type)
    _record_history(db_path, fp_type)


def remove(db_path: Path, name: str, city: str, topic: str,
           fp_type: str = "extraction") -> None:
    delete_false_positive(db_path, name, city, topic, fp_type)
    _record_history(db_path, fp_type)


def build_prompt_section(fps: list[dict], city: str = "", topic: str = "",
                         fp_type: str = "extraction") -> str:
    relevant = [
        fp for fp in fps
        if fp.get("fp_type", "extraction") == fp_type
        and (not city or fp.get("city") == city)
        and (not topic or fp.get("topic") == topic)
    ]
    if not relevant:
        return ""
    if fp_type == "extraction":
        lines = ["\n\nNEGATIVE EXAMPLES — these are NOT valid community groups, do NOT extract them:"]
    else:
        lines = ["\n\nNEGATIVE EXAMPLES — previously flagged incorrect enrichment results:"]
    for fp in relevant:
        lines.append(f'- "{fp["name"]}": {fp["reason"]}')
    return "\n".join(lines)


# ── Prompt version history ────────────────────────────────────────────────────

def _base_prompt(fp_type: str) -> str:
    from .extract import ENRICH_SYSTEM_PROMPT, SYSTEM_PROMPT
    return SYSTEM_PROMPT if fp_type == "extraction" else ENRICH_SYSTEM_PROMPT


def _record_history(db_path: Path, fp_type: str) -> None:
    fps = get_false_positives(db_path)
    type_fps = [fp for fp in fps if fp.get("fp_type", "extraction") == fp_type]
    effective = _base_prompt(fp_type) + build_prompt_section(fps, fp_type=fp_type)

    history = get_prompt_history(db_path, fp_type)
    prev = history[-1]["content"] if history else ""
    if effective == prev:
        return

    append_prompt_history(
        db_path,
        version=len(history) + 1,
        timestamp=datetime.now(timezone.utc).isoformat(),
        content=effective,
        fp_type=fp_type,
        fp_count=len(type_fps),
    )


def load_history(db_path: Path, fp_type: str = "extraction") -> list[dict]:
    return get_prompt_history(db_path, fp_type)


def diff_html(old: str, new: str) -> str:
    old_lines = old.splitlines(keepends=True)
    new_lines = new.splitlines(keepends=True)
    result = []
    for line in difflib.ndiff(old_lines, new_lines):
        if line.startswith("+ "):
            result.append(f'<span class="diff-add">{_esc(line[2:])}</span>')
        elif line.startswith("- "):
            result.append(f'<span class="diff-del">{_esc(line[2:])}</span>')
        elif line.startswith("  "):
            result.append(_esc(line[2:]))
    return "".join(result)


def _esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
