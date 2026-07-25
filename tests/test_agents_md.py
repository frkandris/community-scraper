"""AGENTS.md must stay a faithful copy of CLAUDE.md.

It drifted for months once — still describing the deleted `revalidate` mode — so
divergence is now a test failure, not a discovery weeks later.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from sync_agents_md import AGENTS_MD, render  # noqa: E402


def test_agents_md_is_in_sync_with_claude_md():
    assert AGENTS_MD.exists(), "AGENTS.md is missing — run scripts/sync_agents_md.py"
    assert AGENTS_MD.read_text(encoding="utf-8") == render(), (
        "AGENTS.md is out of sync with CLAUDE.md — run: "
        "PYTHONPATH=. .venv/bin/python scripts/sync_agents_md.py"
    )
