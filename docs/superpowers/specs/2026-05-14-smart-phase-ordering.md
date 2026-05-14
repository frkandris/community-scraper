# Smart run fázis-alapú sorrend

**Dátum:** 2026-05-14

## Probléma

A Smart futás (`run_mode="full"`) jelenleg lineárisan halad városonként: minden `(város, téma)` párnál egyszerre dől el, hogy re-ai kell-e vagy új keresés. Ez azt jelenti, hogy a lista elején lévő nagy városok re-ai és search munkája párhuzamosan halad a kis városokéval — nem kapnak prioritást.

## Cél

A Smart futás három szekvenciális fázisba rendeződik, így a fontos (elöl lévő) városok re-ai feldolgozása hamarabb kész, mielőtt az új keresések elindulnának.

## Tervezett fázissorrend

### 1. fázis — Re-AI
`_run_ai_only` hívás az összes városra, várossorrendben.

- Végigmegy a DB-ben cache-elt oldalakon.
- Ha az extraction fingerprint stale (prompt/modell változott) → újra AI-ozza.
- Ha a fingerprint egyezik → cache hit, gyors átugrás.
- Teljesen új, még nem cache-elt városoknál nincs feldolgoznivaló → átugrás.

### 2. fázis — Search / új lefedettség
`_run_full` hívás az összes városra, várossorrendben.

- Az 1. fázis után a re-ai-zott oldalak friss fingerprint-tel rendelkeznek → extraction cache hit lesz rájuk.
- Ez a fázis ténylegesen az új `(város, téma)` párok search+fetch+extract munkáját végzi.
- Az első (fontos) városok hiányzó témái legelőbb kapnak keresést.

### 3. fázis — Catchup
A meglévő `get_covered_pairs` alapú pótló pass változatlan marad.

## Kódváltozás

**Egyetlen hely:** `scraper/pipeline.py`, `run_pipeline()` függvény.

```python
# Előtte (~309. sor):
total_new, pair_logs = await _run_full(
    cities, topics, config, extractor, cache,
    _skip_scraped, _skip_extracted, run_stats, on_progress, ...
)

# Utána:
reai_new, reai_logs = await _run_ai_only(
    cities, topics, config, extractor, cache,
    _skip_extracted, run_stats, on_progress,
    run_communities=run_communities, run_venues=run_venues, run_persons=run_persons,
)
full_new, full_logs = await _run_full(
    cities, topics, config, extractor, cache,
    _skip_scraped, _skip_extracted, run_stats, on_progress,
    run_communities=run_communities, run_venues=run_venues, run_persons=run_persons,
)
total_new = reai_new + full_new
pair_logs = reai_logs + full_logs
```

A catchup pass (`if run_mode == "full": covered = get_covered_pairs(...)`) változatlan.

## Pair logs kezelés

A re-ai és full fázisok pair_logs-ai kombináltan kerülnek a DB-be. Ez teljesebb képet ad a futásról: látszik, mely párok kaptak re-ai feldolgozást és melyek kerültek keresésre.

## Érintett módok

- Scheduler automatikus futás (`_scheduled_run` in `main.py`) → `run_mode="full"` → érintett
- Manuális Smart trigger (`/api/run`, `run_mode="full"`) → érintett
- Startup run ha `startup_mode="full"` → érintett
- `ai_only` mód → nem érintett (az `if run_mode == "ai_only"` ág változatlan)

## Kockázatok

- **Teljesítmény:** Az `_run_ai_only` fázis egy extra DB olvasással indul (cache-elt oldalak listája). Ez elhanyagolható.
- **run_stats felülírás:** Mindkét fázis írja a `run_stats` dict-et. A 2. fázis felülírja az 1. fázis értékeit, de ez elfogadható — a végső állapot helyes.
