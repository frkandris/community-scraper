import difflib
import json
from datetime import datetime, timezone
from pathlib import Path

FP_TYPES = ("extraction", "enrichment")


def load(data_dir: Path) -> list[dict]:
    fp_file = data_dir / "false_positives.json"
    if not fp_file.exists():
        return []
    try:
        return json.loads(fp_file.read_text(encoding="utf-8"))
    except Exception:
        return []


def _save_all(data_dir: Path, fps: list[dict]) -> None:
    (data_dir / "false_positives.json").write_text(
        json.dumps(fps, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def add(data_dir: Path, name: str, city: str, topic: str, reason: str,
        source_url: str, fp_type: str = "extraction") -> None:
    fps = load(data_dir)
    fps = [fp for fp in fps if not (
        fp["name"] == name and fp["city"] == city
        and fp["topic"] == topic and fp.get("fp_type", "extraction") == fp_type
    )]
    fps.append({
        "name": name,
        "city": city,
        "topic": topic,
        "reason": reason,
        "source_url": source_url,
        "fp_type": fp_type,
        "marked_at": datetime.now(timezone.utc).isoformat(),
    })
    _save_all(data_dir, fps)
    _record_history(data_dir, fps, fp_type)


def remove(data_dir: Path, name: str, city: str, topic: str,
           fp_type: str = "extraction") -> None:
    fps = load(data_dir)
    fps = [fp for fp in fps if not (
        fp["name"] == name and fp["city"] == city
        and fp["topic"] == topic and fp.get("fp_type", "extraction") == fp_type
    )]
    _save_all(data_dir, fps)
    _record_history(data_dir, fps, fp_type)


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


def _record_history(data_dir: Path, fps: list[dict], fp_type: str) -> None:
    hist_file = data_dir / f"prompt_history_{fp_type}.json"
    history: list[dict] = []
    if hist_file.exists():
        try:
            history = json.loads(hist_file.read_text(encoding="utf-8"))
        except Exception:
            history = []

    type_fps = [fp for fp in fps if fp.get("fp_type", "extraction") == fp_type]
    effective = _base_prompt(fp_type) + build_prompt_section(fps, fp_type=fp_type)
    prev = history[-1]["content"] if history else ""
    if effective == prev:
        return

    history.append({
        "version": len(history) + 1,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "content": effective,
        "fp_type": fp_type,
        "fp_count": len(type_fps),
    })
    hist_file.write_text(json.dumps(history, indent=2, ensure_ascii=False), encoding="utf-8")


def load_history(data_dir: Path, fp_type: str = "extraction") -> list[dict]:
    hist_file = data_dir / f"prompt_history_{fp_type}.json"
    if not hist_file.exists():
        return []
    try:
        return json.loads(hist_file.read_text(encoding="utf-8"))
    except Exception:
        return []


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
