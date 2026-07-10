---
type: Integration
title: GA4 Reporting
description: The daily email reads visitor/session/pageview numbers from the GA4 Data API via a service account; property 536914034 covers both domains, split by hostName.
tags: [integration, analytics, ga4, service-account, reporting]
timestamp: 2026-07-10
resource: scraper/report.py
---

# GA4 Reporting

*Google Analytics is the source of truth for visitor numbers in the daily email; the
server-side counter remains as a bot-filtered footnote and fallback.*

## Contract

- **GA4 property**: `536914034` (account `394132821`) — one property tracks both
  kozossegek.com and meetapedia.com; `fetch_ga4_traffic()` splits rows by the
  `hostName` dimension (`www.` stripped, substring-matched to a site).
- **GCP project**: `kozossegek-report` (ID `august-button-501918-d2`), Google
  Analytics Data API enabled.
- **Service account**: `ga4-report@august-button-501918-d2.iam.gserviceaccount.com`,
  added as **Viewer** on the GA4 property (no GCP project roles needed).
- **Env vars** (both required, else GA4 is skipped and the email falls back to the
  server counter): `GA4_PROPERTY_ID`, `GA4_CREDENTIALS_JSON` — the full service
  account key JSON pasted as the value.
- **API call**: `POST https://analyticsdata.googleapis.com/v1beta/properties/{id}:runReport`
  with dims `[hostName]`, metrics `[activeUsers, sessions, screenPageViews]`, bearer
  token from `google.oauth2.service_account` (scope `analytics.readonly`).

## Failure model

`fetch_ga4_traffic()` returns `None` on ANY error (missing env, bad key, API failure)
and logs `ga4_fetch_failed` — the [[daily-report]] then renders the server-side
counter instead. GA4 can never break the email.

## Hard-won rules

- **`GA4_CREDENTIALS_JSON` must be a runtime-only env var in Coolify.** Marking it
  "Available at Buildtime" injects it as a Dockerfile `ARG`, and the multiline JSON
  breaks the build — see [[2026-07-ga4-env-buildtime-failure]].
- The SA key JSON is created and pasted by the owner; sessions must not handle the
  key material.
- GA4 numbers usually exceed the server counter (SPA-less full pageloads, but the
  counter's bot filter is stricter and it only counts public HTML).
