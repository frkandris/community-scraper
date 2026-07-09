---
type: Hack
title: SearchQuotaError Must Be Re-Raised Before the Broad except
description: A provider's `except SearchQuotaError: raise` must precede its `except Exception: return []`, or failover never triggers.
tags: [search, exceptions, failover, ordering]
timestamp: 2026-07-09
resource: scraper/search.py
---

# SearchQuotaError Must Be Re-Raised Before the Broad except

> **Mostly historical since 2026-07-09**: the Serper and Google Playwright clients that
> carried this pattern were removed. The invariant still applies to any future client
> that pairs a broad `except Exception: return []` with quota signaling.

*In `SerperSearchClient.search` and `GooglePlaywrightSearchClient.search`, an explicit `except SearchQuotaError: raise` comes before the generic `except Exception: return []`.*

`SearchQuotaError` is the **only** signal `FallbackSearchClient` catches to roll to the next provider (see [[search-layer]]). If the broad `except Exception` caught it first, the quota error would be converted to `[]`, and the client would never fail over — it would silently return empty results and the paid fallback would never run. The re-raise ordering is a hard-won invariant: the specific handler must precede the catch-all.

Every other exception is intentionally swallowed to `[]` (no failover), which is why a 500 from a search provider does not trigger the fallback either.
