# Smart Run Phase-Based Ordering Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Smart futás (`run_mode="full"`) az AI-újrafeldolgozást (re-ai) az összes városra elvégzi először (várossorrendben), majd utána kerülnek sorra az új keresések — így a lista elején lévő nagy városok adatai frissülnek legelőbb.

**Architecture:** A `run_pipeline()` függvény `else` ágában (run_mode="full") a jelenlegi egyetlen `_run_full` hívás elé egy `_run_ai_only` hívás kerül. A re-ai fázis a DB-ben cache-elt oldalakat dolgozza fel, re-extraktál ahol stale a fingerprint. Ezután a `_run_full` lefut, de az already-extracted oldalak cache hitet kapnak (friss fingerprint), tehát ténylegesen csak az új keresések maradnak. A catchup pass változatlan marad.

**Tech Stack:** Python, pytest, unittest.mock

---

### Task 1: Teszt a fázis-sorrendre

**Files:**
- Create: `tests/test_pipeline_phase_order.py`

- [ ] **Step 1: Írjuk meg a bukó tesztet**

```python
import asyncio
from unittest.mock import AsyncMock, patch, call
from pathlib import Path

import pytest

from scraper.pipeline import (
    CityConfig,
    PipelineConfig,
    TopicConfig,
    run_pipeline,
)


def _cfg(tmp_path: Path) -> PipelineConfig:
    return PipelineConfig(
        searxng_url="http://localhost:8888",
        ollama_url="http://localhost:11434",
        ollama_model="llama3",
        ollama_temperature=0.1,
        ollama_timeout=30,
        ollama_max_text_chars=6000,
        search_results_per_query=5,
        search_max_pages=2,
        search_rate_limit=1.0,
        fetch_timeout=15,
        fetch_min_text_length=100,
        fetch_max_concurrent=3,
        fetch_blocked_domains=[],
        db_path=tmp_path / "scraper.db",
    )


def test_full_mode_runs_reai_before_search(tmp_path):
    """Smart mode must run re-ai phase first, then full search phase."""
    from scraper.db import init_db
    db = tmp_path / "scraper.db"
    init_db(db)

    cities = [CityConfig(name="Budapest", locale="hu", search_variants=[])]
    topics = [TopicConfig(name="running", search_terms={"hu": ["futás"]})]
    cfg = _cfg(tmp_path)

    call_order = []

    async def fake_ai_only(*args, **kwargs):
        call_order.append("ai_only")
        return 0, []

    async def fake_full(*args, **kwargs):
        call_order.append("full")
        return 0, []

    with patch("scraper.pipeline._run_ai_only", side_effect=fake_ai_only), \
         patch("scraper.pipeline._run_full", side_effect=fake_full), \
         patch("scraper.pipeline.get_covered_pairs", return_value=set()), \
         patch("scraper.pipeline.OllamaExtractor"), \
         patch("scraper.pipeline.detect_all"):
        asyncio.run(run_pipeline(cities, topics, cfg, cache=None, run_mode="full"))

    assert call_order[0] == "ai_only", f"Expected ai_only first, got: {call_order}"
    assert "full" in call_order, f"Expected full to be called, got: {call_order}"


def test_ai_only_mode_does_not_run_full(tmp_path):
    """ai_only mode must not trigger the full search phase."""
    from scraper.db import init_db
    db = tmp_path / "scraper.db"
    init_db(db)

    cities = [CityConfig(name="Budapest", locale="hu", search_variants=[])]
    topics = [TopicConfig(name="running", search_terms={"hu": ["futás"]})]
    cfg = _cfg(tmp_path)

    call_order = []

    async def fake_ai_only(*args, **kwargs):
        call_order.append("ai_only")
        return 0, []

    async def fake_full(*args, **kwargs):
        call_order.append("full")
        return 0, []

    with patch("scraper.pipeline._run_ai_only", side_effect=fake_ai_only), \
         patch("scraper.pipeline._run_full", side_effect=fake_full), \
         patch("scraper.pipeline.OllamaExtractor"), \
         patch("scraper.pipeline.detect_all"):
        asyncio.run(run_pipeline(cities, topics, cfg, cache=None, run_mode="ai_only"))

    assert call_order == ["ai_only"], f"Expected only ai_only, got: {call_order}"
```

- [ ] **Step 2: Futtassuk a tesztet, ellenőrizzük, hogy megbukik**

```bash
pytest tests/test_pipeline_phase_order.py -v
```

Várt kimenet: `FAILED` — `test_full_mode_runs_reai_before_search` azért bukik, mert `call_order[0]` jelenleg `"full"`, nem `"ai_only"`.

- [ ] **Step 3: Commit a bukó teszttel**

```bash
git add tests/test_pipeline_phase_order.py
git commit -m "test: add failing test for smart run phase ordering"
```

---

### Task 2: Implementáció — fázis-sorrend bevezetése

**Files:**
- Modify: `scraper/pipeline.py:303-312`

- [ ] **Step 4: Módosítsuk a `run_pipeline` függvényt**

A `scraper/pipeline.py`-ban csöréljük le a `else` ágat (jelenleg ~308–312. sor):

```python
    else:
        total_new, pair_logs = await _run_full(
            cities, topics, config, extractor, cache, _skip_scraped, _skip_extracted, run_stats, on_progress,
            run_communities=run_communities, run_venues=run_venues, run_persons=run_persons,
        )
```

Erre:

```python
    else:
        reai_new, reai_logs = await _run_ai_only(
            cities, topics, config, extractor, cache, _skip_extracted, run_stats, on_progress,
            run_communities=run_communities, run_venues=run_venues, run_persons=run_persons,
        )
        full_new, full_logs = await _run_full(
            cities, topics, config, extractor, cache, _skip_scraped, _skip_extracted, run_stats, on_progress,
            run_communities=run_communities, run_venues=run_venues, run_persons=run_persons,
        )
        total_new = reai_new + full_new
        pair_logs = reai_logs + full_logs
```

A fájl többi része változatlan marad. A catchup pass (`if run_mode == "full": ...`) nem módosul.

- [ ] **Step 5: Futtassuk a teszteket**

```bash
pytest tests/test_pipeline_phase_order.py -v
```

Várt kimenet: mind a két teszt `PASSED`.

- [ ] **Step 6: Futtassuk a teljes tesztcsomag**

```bash
pytest
```

Várt kimenet: minden teszt `PASSED`, nincs regresszió.

- [ ] **Step 7: Commit**

```bash
git add scraper/pipeline.py
git commit -m "feat: smart run runs re-ai phase before search phase

Cities at the top of the list (larger, more important) get their
stale AI extractions refreshed first, before any new searches run.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```
