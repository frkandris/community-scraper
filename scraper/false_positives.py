import difflib
import json
from datetime import datetime, timezone
from pathlib import Path


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


def add(data_dir: Path, name: str, city: str, topic: str, reason: str, source_url: str) -> None:
    fps = load(data_dir)
    # Overwrite if same name+city+topic already marked
    fps = [fp for fp in fps if not (fp["name"] == name and fp["city"] == city and fp["topic"] == topic)]
    fps.append({
        "name": name,
        "city": city,
        "topic": topic,
        "reason": reason,
        "source_url": source_url,
        "marked_at": datetime.now(timezone.utc).isoformat(),
    })
    _save_all(data_dir, fps)
    _record_history(data_dir, fps)


def remove(data_dir: Path, name: str, city: str, topic: str) -> None:
    fps = load(data_dir)
    fps = [fp for fp in fps if not (fp["name"] == name and fp["city"] == city and fp["topic"] == topic)]
    _save_all(data_dir, fps)
    _record_history(data_dir, fps)


def build_prompt_section(fps: list[dict], city: str = "", topic: str = "") -> str:
    relevant = [
        fp for fp in fps
        if (not city or fp.get("city") == city) and (not topic or fp.get("topic") == topic)
    ]
    if not relevant:
        return ""
    lines = ["\n\nNEGATIVE EXAMPLES — these are NOT valid community groups, do NOT extract them:"]
    for fp in relevant:
        lines.append(f'- "{fp["name"]}": {fp["reason"]}')
    return "\n".join(lines)


# ── Prompt version history ────────────────────────────────────────────────────

def _effective_prompt(base_prompt: str, fps: list[dict]) -> str:
    return base_prompt + build_prompt_section(fps)


def _record_history(data_dir: Path, fps: list[dict]) -> None:
    from .extract import SYSTEM_PROMPT
    hist_file = data_dir / "prompt_history.json"
    history: list[dict] = []
    if hist_file.exists():
        try:
            history = json.loads(hist_file.read_text(encoding="utf-8"))
        except Exception:
            history = []

    effective = _effective_prompt(SYSTEM_PROMPT, fps)
    prev = history[-1]["content"] if history else ""
    if effective == prev:
        return

    history.append({
        "version": len(history) + 1,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "content": effective,
        "fp_count": len(fps),
    })
    hist_file.write_text(json.dumps(history, indent=2, ensure_ascii=False), encoding="utf-8")


def load_history(data_dir: Path) -> list[dict]:
    hist_file = data_dir / "prompt_history.json"
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
        # skip "? " hint lines
    return "".join(result)


def _esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
