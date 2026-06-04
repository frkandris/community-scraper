# LLM Prompts With Non-English Examples Bias Output Language

If the `SYSTEM_PROMPT` in `extract.py` contains example values in a specific language (e.g. Hungarian), the LLM will produce descriptions and field values in that language for **all** cities, regardless of the source page language.

## Example

Before fix, the prompt contained strings like:
- `"~50 fő"` (Hungarian for "~50 people")
- `"Heti"` / `"Kéthetente"` (Weekly / Biweekly)
- `"Nyílt csatlakozás"` (Open to all)

Result: Swedish communities (Stockholm) got Hungarian-language descriptions extracted from English/Swedish source pages.

## Fix

Replace all example values with English equivalents:
- `"~50 fő"` → `"~50 members"`
- `"Heti"` → `"Weekly"`
- `"Nyílt csatlakozás"` → `"Open to all"`

Add an explicit instruction: *"Write in the same language as the source page — if the page is in Swedish write in Swedish, etc."*

## Consequence for cache

Changing `SYSTEM_PROMPT` changes `get_extract_fingerprint()`, which invalidates **all** cached extractions. Use the restamp endpoint (see [[init-db-before-prompt-overrides]]) to migrate existing good extractions rather than re-running everything.
