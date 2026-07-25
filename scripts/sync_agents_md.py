#!/usr/bin/env python3
"""Regenerate AGENTS.md from CLAUDE.md.

Two agent-guidance files drifted badly once: AGENTS.md still documented the
`revalidate` run mode months after it was deleted, and knew nothing about the
cost-saver twin schedule. One canonical source (CLAUDE.md) plus this generator
makes that impossible; tests/test_agents_md.py fails when they diverge.

Usage:  PYTHONPATH=. .venv/bin/python scripts/sync_agents_md.py
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CLAUDE_MD = ROOT / "CLAUDE.md"
AGENTS_MD = ROOT / "AGENTS.md"

HEADER = """# AGENTS.md

<!-- GENERATED FILE — do not edit directly.
     Source: CLAUDE.md · Regenerate: PYTHONPATH=. .venv/bin/python scripts/sync_agents_md.py -->

This file provides guidance to coding agents (Codex, Copilot, and friends) working
with code in this repository. It is a verbatim copy of CLAUDE.md.
"""


def render() -> str:
    body = CLAUDE_MD.read_text(encoding="utf-8")
    # Drop the source file's own title + intro line; the header above replaces them.
    lines = body.splitlines()
    if lines and lines[0].startswith("# "):
        lines = lines[1:]
    while lines and (not lines[0].strip() or lines[0].startswith("This file provides guidance")):
        lines = lines[1:]
    return HEADER + "\n" + "\n".join(lines).lstrip("\n") + "\n"


def main() -> int:
    rendered = render()
    current = AGENTS_MD.read_text(encoding="utf-8") if AGENTS_MD.exists() else ""
    if current == rendered:
        print("AGENTS.md already in sync with CLAUDE.md")
        return 0
    AGENTS_MD.write_text(rendered, encoding="utf-8")
    print(f"AGENTS.md regenerated from CLAUDE.md ({len(rendered)} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
