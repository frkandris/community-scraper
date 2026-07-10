# FAQ

Questions that actually came up. Answers link to the full pages.

**Why did my running pipeline disappear?**
Every Coolify deploy RESTARTS the container and kills any in-flight run. Re-trigger it
from the admin (or `POST /admin/api/run` with the right `run_mode`); the collector
cron only self-heals at the next 01:00 UTC. See [[deployment-coolify]].

**I pushed a commit — is it live?**
Not necessarily. A webhook deploy that arrives while another deploy runs FAILS (it may
also just queue — verify per-commit). Check the Deployments list for YOUR commit SHA,
not just app health. See [[2026-07-ga4-env-buildtime-failure]].

**Why is a manual search run so slow?**
`dataforseo_mode: standard` uses the queued task API (minutes of latency) for a 70%
discount. That's the trade, not a bug. See [[dataforseo]].

**Why does editing an extraction prompt cost money?**
The prompt is part of the cache fingerprint — every cached extraction of that family
goes stale and the nightly extractor re-runs it (off-peak, cheap, but not free). See
[[extraction-fingerprint-cache]].

**Why doesn't a re-scrape resurrect a community I hid?**
`hidden` survives `replace_communities_for_topic` by design (2026-07 bug-hunt fix).
Un-hide it via admin, not by re-scraping. See [[not-community-moderation-flow]].

**Why do meetapedia's homepage totals include Hungarian communities?**
Decided 2026-07-09: meetapedia serves HU cities, so its stats match its browsable
content (before that, tile counts were global-minus-HU and contradicted the city count).

**Why do emails come from info@kozossegek.com even for meetapedia?**
Resend's free plan verifies exactly one domain. See [[resend-email]].

**Where do the visitor numbers in the daily email come from?**
GA4 Data API (property 536914034) when `GA4_PROPERTY_ID` + `GA4_CREDENTIALS_JSON` are
set; otherwise the bot-filtered server counter. See [[ga4-reporting]] and
[[daily-report]].

**A city's data is bad — how do I redo it from scratch?**
`POST /admin/api/reset-city` wipes its communities/venues/persons/search_cache/pages,
because green (done) pairs are otherwise skipped forever. Then run a collect + extract
cycle. See [[run-modes-and-startup]].

**Coolify says high disk usage — what now?**
Old Docker images after a deploy-heavy day. Two prune commands, volumes untouched.
Runbook: [[coolify-disk-cleanup]].

**Why can't the browser reach /admin while the public site works?**
`/admin` sits behind HTTP Basic auth (`ADMIN_PASSWORD`); a fresh browser session needs
the credentials again. The public site and `/healthz` need none.

*(Wikilinks here are informational; faq.md is outside the linted `pages/` graph.)*
