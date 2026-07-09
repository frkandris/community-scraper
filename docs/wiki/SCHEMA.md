# Wiki Schema

This is the LLM-maintained knowledge base for the **community-scraper** project. It
combines two conventions:

- **Karpathy LLM Wiki pattern** — a persistent, interlinked markdown wiki that an LLM
  incrementally builds and maintains, with `index.md` (catalog), `log.md` (operation
  log), and ingest / query / lint workflows.
  <https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f>
- **Open Knowledge Format (OKF) v0.1** — every concept page carries YAML frontmatter
  with a required `type` field; links are directed, untyped graph edges; consumers
  tolerate missing fields and broken links gracefully.
  <https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md>

The bundle targets `okf_version: "0.1"` (declared in `index.md`).

## Directory structure

```
docs/wiki/
  SCHEMA.md       — this file: conventions + LLM instructions (not a concept page)
  index.md        — OKF directory listing / content catalog (one line per page)
  log.md          — date-grouped operation log, newest first
  sources/        — immutable raw inputs (incident notes, research, pasted docs)
  pages/
    architecture/ — system design, data flow, component relationships
    subsystems/   — one page per code module: what it does, key funcs, gotchas
    data-model/   — entities, DB tables, identity keys, models
    concepts/     — domain concepts (the problem, not just the code)
    decisions/    — why we chose X over Y (ADR-style)
    hacks/        — non-obvious tricks, workarounds, hard-won knowledge
    post-mortems/ — incident reports: root cause + fix + lesson
    operations/   — runbooks: how to run, deploy, add a city/topic, migrate
    seo/          — search-indexing strategy, canonical/sitemap/robots rules
```

Categories are open — add a new one when a cluster of pages warrants it. `index.md` and
`log.md` are OKF **reserved filenames** and must not be used for concept documents.

## Page format (OKF concept document)

Every `.md` under `pages/` MUST begin with YAML frontmatter:

```yaml
---
type: <concept kind>          # REQUIRED, e.g. Hack, Post-mortem, Decision,
                              #   Concept, Architecture, Subsystem, Data-model, Runbook
title: <human-readable name>  # recommended
description: <one sentence>    # recommended — mirrored into index.md
tags: [kebab, case, list]     # recommended
timestamp: 2026-07-09         # recommended — ISO 8601 last-modified date
resource: scraper/extract.py  # optional — canonical source file/URI the page documents
---
```

After the frontmatter:

1. `# Title` (H1)
2. A single-sentence summary in *italics*.
3. Body sections. OKF conventional headings when they apply: `# Schema` (structured
   fields/columns), `# Examples`, `# Citations` (external sources).

Conventions:

- One concept per file, named in `kebab-case.md`.
- Keep pages short (< 300 lines); split when they grow.
- No duplication — link instead of repeating.
- `consumers must tolerate unknown types` — pick a descriptive `type`, don't over-police it.

## Cross-linking

Two supported forms; prefer the first:

- **Wikilink (project convention):** `[[page-name]]` — the filename without `.md`. Cheap
  to write; the index resolves them. A `[[link]]` to a page that doesn't exist yet is a
  valid TODO marker, not an error.
- **OKF bundle-relative:** `[label](/pages/hacks/foo.md)` — stable across moves; use when
  an exact path matters (e.g. from outside `pages/`).

Links are directed, untyped edges; the surrounding prose expresses the relationship.

## index.md

OKF directory listing. One bullet per page, grouped by category:

```
- [[page-name]] — one-line description (mirrors the page's `description` frontmatter)
```

Root `index.md` frontmatter declares `okf_version: "0.1"`.

## log.md

Date-grouped, **newest first**. Each day is an H2; entries use `**Creation**` /
`**Update**` / `**Deprecation**` prefixes (convention, not required):

```markdown
## 2026-07-09
- **Creation**: Added [[seo-cross-domain-canonical]] documenting the HU-page canonical fix.
- **Update**: Refreshed [[search-provider-fallback-chain]] for the Playwright provider.
```

## LLM operations

### Ingest
When a source lands in `sources/`, read it, then create or update ~5–15 pages, add
cross-links, mirror each page's `description` into `index.md`, and prepend a dated
`log.md` entry.

### Query
Search the wiki first; answer with page citations. If the answer surfaces durable new
knowledge (a comparison, a connection, an analysis), file it as a new page.

### Lint
Periodically check for: contradictions between pages, stale claims the code has moved
past, orphan pages (no inbound links), missing cross-references, missing frontmatter
`type`, and important concepts lacking a page.

## What belongs here

Non-obvious knowledge: design decisions and their rationale, data flow and invariants,
gotchas that surprised us, incident root-causes, and domain concepts. Subsystem pages may
restate structure for orientation, but the value is the *why* and the *traps*, not a line
-by-line paraphrase of the code.

## What does NOT belong here

- Secrets or credentials.
- Verbatim git history (use `git log` / `git blame`).
- Transient in-progress state.
- Generic best practices (link out instead).

## Doc drift warning

`PROJECT.md` and `README.md` at the repo root have **drifted** from the current code (they
still describe the retired Serper→Brave→SearXNG search chain and the Ollama extractor).
Treat the code and this wiki as authoritative; see [[doc-drift-project-readme]].
