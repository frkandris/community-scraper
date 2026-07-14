---
type: Subsystem
title: Daily Report Email
description: report.py builds one email per UTC day — GA4 visitors, per-site diffs, run outcomes, and current stock totals — sent via Resend at 04:30 UTC or on demand.
tags: [subsystem, report, email, traffic, analytics]
timestamp: 2026-07-14
resource: scraper/report.py
---

# Daily Report Email

*One email per day answers "what happened yesterday and where does the database
stand" — visitors, diffs, runs, and totals, split Hungarian / international.*

## Pipeline

1. **Trigger**: cron `30 4 * * *` UTC (`schedule.report_enabled` in settings) or
   `POST /admin/api/send-daily-report`. Default day = yesterday (UTC).
2. **Data**: `get_daily_summary(db, start_iso, end_iso, hu_cities)` in `scraper/db.py`
   computes per-scope diffs (new/changed communities via `community_history` joins
   with the `__created__`/MIN(changed_at) guard from
   [[history-created-sentinel-overcounting]], venues, persons, pages, searches),
   run outcomes with `search_failed`/`extract_failed` counters plus the persisted
   top-level `runs.error`, and a `stock` dict —
   current totals per scope (communities, venues, persons, cached/extracted pages,
   covered pairs). Scope split: city ∈ `hu_cities` → `hu`, else `intl`.
3. **Traffic**: [[ga4-reporting]] numbers are primary (visitors/sessions/pageviews per
   site); the server-side counter (`traffic_daily`/`traffic_visitors` tables, fed by a
   bot-filtering HTTP middleware hashing `day|ip|ua`) is the fallback and footnote.
4. **Render**: `build_report_html()` — sections: Látogatók (GA4), Változások (diff
   table), Futások (runs with failure notes), Állomány (current stock table). Labels
   are self-explanatory Hungarian ("város–téma páros", never bare "pár").
5. **Send**: [[resend-email]] from `info@kozossegek.com` to `REPORT_EMAIL` (fallback
   `FEEDBACK_EMAIL`). Subject: `[közösségek] Napi összefoglaló {day} — {n} új
   közösség, {m} látogató`.

## Design points

- **Diffs AND stock**: the Változások table shows what changed yesterday; the
  Állomány table (added 2026-07-09) shows absolute current totals in the same
  Magyar/Nemzetközi/Össz layout — both views in one email.
- `build_report_html` accepts summaries without a `stock` key (falls back to the old
  totals) so it never breaks on older data shapes.
- Middleware counts **public HTML GETs only** — bot user-agents, `/admin`, static
  assets, and utility paths are excluded.
- Everything degrades silently: no Resend key → skip with log; no GA4 env → server
  counter; empty day → zeros, email still sent.
- Scheduled/startup exceptions are HTML-escaped and displayed as `futási hiba`; a
  zero-pair failed run therefore carries its actionable cause in the email.
