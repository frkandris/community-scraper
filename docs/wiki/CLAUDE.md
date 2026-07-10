# Wiki Maintenance Schema

This file tells any LLM session **when** to capture knowledge into this wiki and
**how**. The page format itself (OKF frontmatter, linting, categories) is specified in
[SCHEMA.md](SCHEMA.md) — read both before editing wiki content.

## Layers

- **Sources (read-only ground truth)**: the codebase, `git log`/`git show`, the
  Coolify dashboard (deploys, env vars, server state), production admin UI, and
  `sources/` for raw pasted material. Never edit sources to match the wiki.
- **The wiki (writable)**: everything under `docs/wiki/` — a compiled, cross-linked
  artifact. When wiki and source disagree, the source wins; fix the wiki and log it.
- **The schema**: this file + [SCHEMA.md](SCHEMA.md).

## When to update (triggers)

| Event | Where it goes |
|---|---|
| New module / runtime surface | `pages/subsystems/` |
| Design choice with alternatives | `pages/decisions/` — capture the **why** |
| Non-obvious trick or workaround | `pages/hacks/` |
| Bug with surprising root cause, or incident | `pages/post-mortems/` (Symptom / Root cause / Fix / Lessons) |
| External service touched (contract or quirk learned) | `pages/integrations/` |
| "What to do when X" knowledge | `pages/operations/` (runbook: exact commands) |
| Domain concept clarified | `pages/concepts/` |
| Schema/table/identity change | `pages/data-model/` |
| Indexing/canonical/sitemap change | `pages/seo/` |
| New domain term | `glossary.md` |
| Question asked twice | `faq.md` |
| **Always** | one line in `log.md` (newest date section first) |

## When NOT to update

- Trivial changes: renames, formatting, lint fixes, dependency bumps.
- Anything already stated in the root `CLAUDE.md` — link to it instead.
- Facts trivially derivable from reading one function.
- Speculative plans — the wiki describes **what is**, not what might be.

## Discipline

- Wiki updates land in the **same commit** as the triggering code change.
- Every non-obvious claim carries provenance: `file.py:line`, commit SHA, or date.
- Dates are absolute (`2026-07-10`), never "recently".
- Contradictions are flagged in-page, not silently overwritten.
- A smaller true wiki beats a padded one.
- Before committing wiki changes run:
  `PYTHONPATH=. .venv/bin/python scripts/lint_wiki.py`
  (structure, index mirroring, link graph, orphans, log ordering). The same checks run
  in the test suite.
- `log.md` uses a `union` merge driver (`.gitattributes`) so concurrent sessions can
  both append.
