# Meetapedia

**A directory of local community groups — running clubs, choirs, board-game nights,
climbing crews, book circles — collected from the open web, city by city.**

Two sites, one project:

- **[meetapedia.com](https://meetapedia.com)** — the international edition, cities worldwide, English (plus other languages).
- **[közösségek.com](https://kozossegek.com)** — the Hungarian edition: the same directory in Hungarian, for Hungarian cities.

## Why it exists

Finding a local group to join is oddly hard. The information exists — on club pages,
municipal sites, parish newsletters, sports association listings — but it is scattered
across thousands of pages nobody thinks to search, in the local language, often without
so much as a meeting time on the front page.

Meetapedia reads those pages so you don't have to. For every city × interest pair it
searches the open web, downloads what it finds, and uses a language model to pull out the
few things that actually matter: what the group is, where and when it meets, and how to
reach it. Groups you can't actually join — commercial classes, one-off events, dead pages —
are filtered out.

At the time of writing it tracks 774 cities across 63 countries and 36 interest topics.

## Who makes it

A hobby project by **[P. Tóth András](https://www.linkedin.com/in/ptothandras/)**, built
and run in the open. Nothing here is a company, a product, or a business — it is one
person's answer to "why is this so hard to look up?"

Everything is public: the pipeline, the extraction prompts, the operational history, and
the engineering wiki. If a listing on the site looks wrong, you can read the exact code
path that produced it — every public page links back to the source it was extracted from.

## How it works

For each `(city, topic)` pair:

1. Search queries are built in the city's own language and sent to a search API.
2. Result pages are fetched and stripped to clean text (social media domains are skipped).
3. A language model extracts structured records — communities, venues, and people.
4. Records are deduplicated, quality-gated, and stored in SQLite.
5. A FastAPI app serves both domains from a single container.

Everything is aggressively cached and fingerprinted, because the whole thing runs on a
hobby budget: pages are downloaded once, extractions are re-run only when the prompt or
model changes, and the expensive LLM work is scheduled inside the provider's off-peak
discount window.

## Documentation

The real documentation is the **[engineering wiki](docs/wiki/index.md)** — an LLM-maintained
knowledge base of how the system actually behaves, including the parts that went wrong.

Good starting points:

| Page | What it covers |
|---|---|
| [End-to-end walkthrough](docs/wiki/pages/architecture/end-to-end-pair-walkthrough.md) | One city × topic pair traced from scheduler wake-up to public page |
| [Two domains, one container](docs/wiki/pages/architecture/two-domain-single-container.md) | How one app serves both sites |
| [Pipeline orchestration](docs/wiki/pages/subsystems/pipeline-orchestration.md) | Run modes, done-pair filtering, city priority |
| [Extraction layer](docs/wiki/pages/subsystems/extraction-layer.md) | Prompts, schemas, fingerprint-keyed caching |
| [Cost-saver schedule](docs/wiki/pages/operations/cost-saver-schedule.md) | Why collection and extraction run at different times of day |
| [Post-mortems](docs/wiki/index.md#post-mortems) | Every incident that taught the system something |
| [Glossary](docs/wiki/glossary.md) · [FAQ](docs/wiki/faq.md) | Domain vocabulary and recurring questions |

`CLAUDE.md` in the repo root is the working brief for coding agents (and a decent
orientation for humans): commands, architecture, and the patterns that are easy to get
wrong. `AGENTS.md` is a generated copy of it.

## Running it yourself

```bash
pip install -e ".[dev]"
PYTHONPATH=. pytest              # tests
.venv/bin/ruff check scraper/    # lint
```

The deployment is Docker on Coolify (Hetzner); only `/app/data` (SQLite) and
`/app/config` (YAML) are persisted. `ADMIN_PASSWORD` is required; `DEEPSEEK_API_KEY`,
`DATAFORSEO_LOGIN`/`DATAFORSEO_PASSWORD`, `RESEND_API_KEY` and the GA4 credentials are
optional — missing keys degrade to a no-op rather than an error. See
[deployment](docs/wiki/pages/operations/deployment-coolify.md) for the full list.

## Data quality

Every record is machine-extracted from a public web page. Information can be incomplete,
outdated, or plain wrong; nothing is verified by hand. Always check with the community
itself before showing up. Each public page carries a link to its source, a "report this"
button, and an edit-suggestion form — corrections are reviewed and applied.
