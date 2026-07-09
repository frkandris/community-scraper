---
type: Hack
title: An Empty-String Prompt Override Silently Reverts to Default
description: get_prompt uses `or`, so an override set to "" is falsy and falls back to the built-in prompt — you cannot blank a prompt via override.
tags: [prompts, overrides, gotcha, admin]
timestamp: 2026-07-09
resource: scraper/extract.py
---

# An Empty-String Prompt Override Silently Reverts to Default

*`get_prompt(key)` returns `_PROMPT_OVERRIDES.get(key) or PROMPT_KEYS[key]()`. The `or` (not a presence check) means an override of `""` is falsy and the built-in default is used instead.*

You cannot blank out a prompt from the admin `/admin/prompts` editor — saving an empty string silently restores the default. To truly shorten a prompt, save non-empty content. Also note `PROMPT_KEYS` has no `enrich_user` key, so the enrichment user message is hard-coded inline and is **not** overridable at all. See [[extraction-layer]].
