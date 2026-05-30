# Extraction Fingerprint Cache

*Every page extraction is keyed by a SHA-256[:12] hash of `SYSTEM_PROMPT + model_name`, so changing the prompt or model automatically invalidates all cached extractions.*

## The mechanism

`cache_pages` table has an `extract_fingerprint` column (TEXT). When a page is extracted:
1. Compute `fingerprint = sha256(system_prompt + model_name)[:12]`
2. Store result alongside the fingerprint
3. On next run: if stored fingerprint matches current, skip extraction (cache hit)

If the admin edits a prompt via `/admin/prompts`, the fingerprint changes → all pages get re-extracted on the next `ai_only` or `full` run. No manual cache invalidation needed.

## Implementation

`extract.py:_prompt_hash(text)` — the hash function  
`extract.py:get_extract_fingerprint(model)` — public helper for the current fingerprint  
`_ApiExtractor.model_fingerprint` property — per-extractor instance  
`FallbackExtractor.model_fingerprint` — delegates to first available primary

## Gotcha: Prompt overrides affect all cached extractions

`_PROMPT_OVERRIDES` is an in-memory dict loaded from the DB at startup and updated live via `set_prompt_override()`. Any change causes every cached extraction to become stale. On a large database this triggers a full re-extraction run — intentional but potentially slow.

## Coverage page states that depend on this

The coverage page uses fingerprint data to distinguish:
- Blue ✓ = extracted with current fingerprint, 0 communities found
- Amber ~ = extracted with old fingerprint (or just searched, never extracted)

## Related

- [[pipeline-run-modes]]
- [[prompt-overrides]]
