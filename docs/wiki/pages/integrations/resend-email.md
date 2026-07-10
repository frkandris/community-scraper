---
type: Integration
title: Resend Email
description: All outbound email (feedback routes + daily report) goes through Resend from info@kozossegek.com; the free plan allows one verified domain, so meetapedia.com has no sender identity.
tags: [integration, email, resend, dns, feedback]
timestamp: 2026-07-10
resource: scraper/report.py
---

# Resend Email

*One Resend account sends every email the app produces. kozossegek.com is the single
verified sender domain; upgrading (~$20/mo) would be needed to add meetapedia.com.*

## Contract

- **Env vars**: `RESEND_API_KEY` (auth), `RESEND_FROM` (sender, set to
  `info@kozossegek.com` in Coolify since 2026-07-09), `FEEDBACK_EMAIL` (recipient for
  feedback routes), `REPORT_EMAIL` (daily-report recipient; falls back to
  `FEEDBACK_EMAIL`). All optional — missing config means silent no-op.
- **Consumers**: `/subscribe`, `/report-not-community`, `/suggest-edit`,
  `/claim-community` (in `scraper/web/app.py`) and the [[daily-report]] sender in
  `scraper/report.py`.

## Domain verification (done 2026-07-09)

kozossegek.com is **Verified** in Resend via four Cloudflare DNS records:

| Record | Name | Value |
|---|---|---|
| TXT | `resend._domainkey` | DKIM public key |
| MX | `send` | `feedback-smtp.eu-west-1.amazonses.com` (prio 10) |
| TXT | `send` | SPF (`v=spf1 include:amazonses.com ~all`) |
| TXT | `_dmarc` | DMARC policy |

## Quirks and constraints

- **Free plan = 1 domain.** meetapedia.com is NOT registered; adding it requires a
  paid plan (~$20/mo). Until then every email — including meetapedia-related ones —
  is sent from `info@kozossegek.com`. Decision deliberately deferred to the owner.
- Before verification the default sender was `onboarding@resend.dev`; replies and
  deliverability both improve with the verified domain.
- Emails sent to a service account or non-existent mailbox bounce silently — when
  granting GA4 access to the reporting service account, "Notify by email" must stay
  unchecked (see [[ga4-reporting]]).
