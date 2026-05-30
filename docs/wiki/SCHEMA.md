# Wiki Schema

This is the LLM-maintained knowledge base for the community-scraper project, following the Karpathy LLM Wiki pattern.

## Directory structure

```
docs/wiki/
  SCHEMA.md       — this file: conventions and LLM instructions
  index.md        — content catalog (one line per page)
  log.md          — append-only chronological operation log
  sources/        — immutable raw inputs (notes, incident reports, research)
  pages/
    architecture/ — system design, data flow, component relationships
    hacks/        — non-obvious tricks, workarounds, hard-won knowledge
    post-mortems/ — incident reports with root cause and fix
    decisions/    — why we chose X over Y (ADR-style)
    concepts/     — key domain concepts explained
```

## LLM operations

### Ingest
When a source is added to `sources/`, read it and:
1. Create or update ~5-15 wiki pages in `pages/`
2. Add cross-references with `[[page-name]]` syntax
3. Append an entry to `log.md`
4. Update `index.md`

### Query
When asked a question, search wiki pages first, synthesize an answer with page citations, and if the answer reveals new knowledge worth keeping, create a new page.

### Lint
Periodically check for:
- Contradictions between pages
- Stale claims (things that changed in the codebase)
- Orphaned pages (no incoming links)
- Missing cross-references

### Maintenance
- `index.md`: one line per page — `- [[name]] — one-line description`
- `log.md`: append-only — `YYYY-MM-DD | operation | brief description`

## Page conventions

- One concept per file, named in `kebab-case.md`
- Title as H1, then a single-sentence summary in italics
- Cross-links as `[[page-name]]` (the filename without `.md`)
- Keep pages short (under 300 lines); split when they grow large
- No duplication — link instead of repeating
- Sources cited at the bottom under `## Sources`

## What belongs here

**Hacks**: non-obvious tricks, workarounds for specific bugs, unintuitive behavior that surprised us.  
**Post-mortems**: what broke, why, how we fixed it, what we learned.  
**Decisions**: why we chose one approach over alternatives — especially when the choice looks weird.  
**Architecture**: how components fit together, data flow, constraints.  
**Concepts**: domain concepts (not just what the code does, but what the problem domain is).

## What does NOT belong here

- Information derivable by reading the code
- Git history (use `git log` / `git blame`)
- Temporary state or in-progress work
- Generic best practices (link to external resources instead)
- Anything already in CLAUDE.md
