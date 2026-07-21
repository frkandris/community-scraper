# Changelog

## 2026-07-21

- **AI-only OOM javítás**: az extractor többé nem tölti memóriába a teljes `cache_pages` raw szövegállományát; mindig csak az aktuális város–téma pár oldalait olvassa be
- **DataForSEO queue javítás**: a standard queue high priority (`priority: 2`, ~$1.2/1K), mert a normál queue 45 perces garantált ideje meghaladhatja az 5 perces kliens-timeoutot
- **Futásállapot**: provider-hibás futás nem kap többé zöld pipát; a graceful cancellation eltárolódik, a befejezetlen DB-sor pedig restart/OOM diagnózist kap a napi riportban

## 2026-07-14

- **Saver collector javítás**: a `search_only` a fetch batch után azonnal kilép, így nem olvas extraction cache-t és nem írhat vissza közösségeket, helyszíneket vagy személyeket
- **Terminális collection marker**: a `search_cache.collected_at` csak a teljes URL-batch megkísérlése után áll be; a tartós fetch-hibák nem járatják újra naponta a teljes magyar állományt, a megszakított párok viszont folytatódnak
- **Svéd prioritás**: a napi, időablakos saver futások sorrendje Svédország → világ → Magyarország
- **Látható futási hibák**: a `runs.error` eltárolja az ütemezett/startup kivételt, és a napi email kiírja annak HTML-escape-elt szövegét

## 2026-05-30 (session 2)

- **290 svéd önkormányzat**: az összes svéd közigazgatási egység felvéve `config/cities.yaml`-ba; Svédország a pipeline-ban Magyarország után, a többi ország előtt fut (hu → se → intl három egymást követő `run_pipeline()` hívással `main.py`-ban)
- **Coverage oldal** (`/admin/coverage`): ország-választó legördülő, városonkénti × témánkénti mátrix, 5 cella-állapot (nem keresett / régi FP / aktuális FP+0 találat / van közösség / éppen fut), „Ugrás az aktív sorhoz" gomb, a témacímek 90°-kal elforgatva; aktív sor kiemelése teljesen JS-alapú (5 mp-es refresh), nem szerver-oldali snapshot
- **Pipeline kihagyás**: `get_fully_processed_pairs(db_path, current_fp)` egyetlen SQL lekérdezéssel szűri ki azokat a párokat, ahol a `search_cache` megvan és minden `cache_pages` sor az aktuális `extract_fingerprint`-et viseli — ezek teljesen kimaradnak a hurokból (sem log, sem UI-update)
- **`on_pair_start` callback**: `run_pipeline()`, `_run_full()`, `_run_ai_only()` és a manuális indítás mindegyike (`app.py`, `main.py`) frissíti `app_state.current_city`/`current_topic`-ot; `finally`-ban törlődik
- **Email értesítések**: Resend integráció `/subscribe`, `/report-not-community`, `/suggest-edit`, `/claim-community` route-okon; `RESEND_API_KEY` + `FEEDBACK_EMAIL` + `RESEND_FROM` env var-ok
- **LLM wiki** (`docs/wiki/`): Karpathy-féle wiki minta — hacks, post-mortemek, döntések, architektúra oldalak `[[link]]` kereszthivatkozásokkal; `index.md` katalógus, `log.md` append-only napló, `SCHEMA.md` szabályok
- **`search_ttl_days: 3650`**: a TTL gyakorlatilag kikapcsolva — egyszer beindexelt párok nem futnak újra; a cél az egész világ lefedése, nem a frissítés

## 2026-05-30

- **Random közösség sorrend**: publikus oldalakon (városlap, topic, felfedezés, főoldal minták) a közösségek minden betöltéskor véletlenszerű sorrendben jelennek meg — `random.shuffle()` az alkalmazás rétegben, a DB lekérdezések változatlanok
- **Description re-AI** (`/admin/maintenance`, Moderation menüben): maintenance oldal, amely újrafuttatja az AI-t a leírás nélküli közösségekre. Kétlépéses logika: (1) re-extract a meglévő source URL-ből (cache-ből vagy live fetch), (2) ha leírás még mindig üres, Google/DataForSEO/Serper keresés a `"közösség neve" város` query-val, top 5 találat fetchelése és extrakciója — természetesen segít a rossz városhoz rendelt közösségek javításában is. Élő progress bar, háttérben fut (`asyncio.create_task`)
- **Aktivitás timeline a Stats oldalon**: `get_activity_timeline(db_path, period)` — 24h (óránként), 7d (naponként), 12m (havonként) nézetek; 8 oszlop: Scrape, Extract, Enrich scrape, Enrich AI, Új közösség, Változás, Új helyszín, Új ember. JS-sel töltődik be JSON endpointról, period-váltó tabokkal
- **`person_history` overcounting javítás**: a `delete_leader_persons_for_community` + `upsert_persons` ciklus minden AI futásnál új `__created__` bejegyzést generált a leader personoknak. Fix: `MIN(changed_at) GROUP BY person_id` — minden személy csak az első megjelenésekor számít újnak. Ugyanez a védelmi fix a `venue_history`-ra is alkalmazva

## 2026-05-29

- **Google Playwright keresés**: `GooglePlaywrightSearchClient` bevezetve elsődleges keresési forrásként — headless Chromium scrapolja a Google találatokat API kulcs nélkül, 8 mp várakozással kérések között. CAPTCHA esetén automatikusan DataForSEO-ra vált. Fallback lánc: Google Playwright → DataForSEO → Serper
- **Automatikus futtatások letiltva**: cron ütemező regisztrálva, de aktív jobok nélkül — a pipeline csak gombnyomásra indul
- **`auto_run_on_startup` config flag**: `config/settings.yaml → schedule.auto_run_on_startup: true/false` vezérli, hogy deploy után automatikusan elindul-e a pipeline (alapértelmezés: `true`)
- **Dashboard gyorsítás**: 5 drága DB lekérdezés eltávolítva a dashboard betöltésből (revalidation count, scope stats ×2, search cache counts) — a Run Now kártyákról eltűntek a számok
- **Person extraction optimalizáció**: ha egy URL-hez 0 közösség tartozik, a person cache lookup és AI hívás ki van hagyva — megtakarít egy DB olvasást URL-enként (az URL-ek többsége 0 közösséget ad)
- **Admin stats oldal** (`/admin/stats`): adatminőség statisztikák témánként és városonként

## 2026-05-15

- **Multi-domain support**: `közösségek.com` (HU cities, HU UI) and `meetapedia.com` (all cities, EN UI) served from one container
- `_detect_site(request)` in `i18n.py` — Host-header-based domain detection
- `_site_cities(request)` in `app.py` — per-domain city filtering (HU-only vs. all)
- Site-aware home stats cache keyed by domain (`dict[str, dict]`)
- All public templates updated to use `{{ site_name }}`, `{{ site_url }}`, `{{ lang }}`, `{{ locale }}` variables
- International cities pipeline run added in scheduler and startup after HU run
- **meetapedia.com site fixes**: English URL paths (`/about`, `/map`, `/explore`, `/cities`, `/submit-community`), flag language selector in nav (far right of search), world-view map default, country-grouped About page, no hardcoded Hungarian strings
- **Search chain simplified**: DataForSEO → Serper only (SearXNG, Brave Search, DuckDuckGo removed)
- **Extractor chain simplified**: DeepSeek → Groq only (Ollama removed)
- **SVG favicon**: orange rounded square with three-person silhouette, served from `/static/favicon.svg`
- **Homepage**: hero label removed; "Cities near you" geolocation section (client-side haversine, localStorage cache); country-grouped popular cities for meetapedia
- **Site-aware sitemap.xml**: meetapedia generates English paths, közösségek generates Hungarian paths
- **robots.txt**: explicit `facebookexternalhit Allow: /` block added
- **Dockerfile HEALTHCHECK**: `--start-period=60s` so Coolify health checks survive slow startup
- **Social media domains blocked**: Facebook, Instagram, TikTok, LinkedIn, YouTube, Reddit, X moved from `playwright_domains` to `blocked_domains` — Playwright was launching Chromium for login-walled sites, causing 91% CPU / 43 GB disk I/O per run

## 2026-05-14

- **Smart run fázis-alapú sorrend**: a Smart futás (`run_mode="full"`) először az összes városra lefuttatja az AI-újrafeldolgozást (stale fingerprint esetén), csak ezután kerülnek sorra az új keresések — a lista elején lévő nagy városok adatai frissülnek legelőbb

## 2026-05-12

- **Stop gomb javítás**: a scheduler és revalidate futásokat most már a stop gomb le tudja állítani — `asyncio.create_task()` + `app_state._run_task` minta; `BackgroundTasks` lecserélve
- **Revalidate logika**: csak azokat validálja újra, ahol `revalidate_fingerprint` elavult (NULL rekordokat kihagyja)
- **Revalidate scope**: Hungary/város szűrő most ténylegesen átadódik a revalidate indításnál
- **Re-AI folyamatban lévő szám**: dashboard kártyán megjelenik, hány rekorden fog lefutni (fingerprint alapján)
- **Futtatási mód badge**: "Running: Smart" / "Running: Re-AI" / "Running: Revalidate" / "Running: Full rebuild" formátum
- **Tech téma**: új érdeklődési kategória programozós/tech meetupokhoz (16 nyelvű keresési kifejezések)
- **Admin dark mode**: hold/nap gomb a navigációban, localStorage perzisztencia, Tailwind CDN `darkMode: 'class'` konfig
- **Dashboard gyorsítás**: `get_scope_stats` lekérdezés nem deszializál JSON blob-okat — `scraped_at IS NOT NULL` + valódi fingerprint oszlopok
- **Run gomb UX**: form POST → `fetch()` AJAX, az oldal a dashboardon marad navigáció nélkül
- **Logs oldal gyorsítás**: a history JSON endpoint-ról töltődik be `DocumentFragment`-tel, nem szerveroldalon renderelve

## 2026-05-11

- **AI recategorize**: új admin menüpont — az AI végigmegy az "other" kategóriás közösségeken, ≥85% konfidenciánál automatikusan átsorolja, 50–85% között jóváhagyásra teszi
- **Vállalkozás téma**: új érdeklődési kategória hozzáadva (keresőkifejezések + ikon + OG kép)
- **Reddit Playwright**: reddit.com hozzáadva a Playwright-domainek listájához; proper user-agent + 3s várakozás SPA-khoz
- **Személy és helyszín oldalak újratervezve**: breadcrumb navigáció, fejléc kártya, `max-w-3xl` elrendezés — megegyezik a közösség oldal stílusával
- **Város chip a tag keresésnél**: `/felfedezes?tag=...` kártyákon megjelenik a város, belinkelve
- **Helyszín típusok magyarul**: "cultural center" → "Művelődési ház" stb., az összes nyilvános sablonban
- **OG képek ékezetes karakterek**: TrueType (Arial Unicode) font váltás — korábban négyzeteket rajzolt Pillow
- **Közösség beküldés**: publikus beküldési form (`/beküldes`), admin jóváhagyás, Re-AI gomb újraextraháláshoz
- **Keresés (`/kereses`)**: egységes keresés közösségek, helyszínek és emberek között
- **Személy és helyszín oldalak**: `/varos/emberek/nev` és `/varos/helyszinek/nev` dedikált részletoldalak
- **Személy szerepek deduplikáció**: ugyanaz a személy több szerepkörrel egy közösségben már nem jelenik meg duplikáltan
- **SEO javítások**: robots.txt, canonical URL-ek, meta description, noindex forrás oldalak
- **Másoló gomb a log box alatt**: mobil-barát elhelyezés

## 2026-05-10

- **Duplikátum kezelés**: admin `/admin/duplicates` oldal — AI-asszisztált összeolvasztás, kézi jelölés, háttérfolyamat
- **Szerkesztési kérelmek**: publikus szerkesztési form közösség oldalakon + admin `/admin/edit-requests` jóváhagyó felület
- **Open Graph meta tagek**: OG + Twitter Card tagek az összes nyilvános oldalon
- **Admin navigáció**: Results / Moderation / System dropdown csoportokba rendezve
- **Fuzzy dedup javítások**: városnév szűrés, 0.85-ös küszöb, generikus szavak szűrőlista bővítve
- **Indításkori futtatás**: deploy/startup után automatikusan elindul egy "smart run"

## 2026-05-09

- **Teljes UI redesign**: "Warm Editorial Minimalism" — terrakotta gradient sávok, kártya-árnyékok
- **Magyarország-fókusz**: explore oldal és statisztikák csak HU adatokat mutatnak
- **Helyszín és ember oldalak**: `/helyszinek`, `/emberek` publikus listázó oldalak szűrőkkel
- **Revalidáció**: meglévő közösségek újra-ellenőrzése az aktuális prompttal
- **Nem-közösség jelölés**: publikus "Nem közösség" gomb + admin kezelőfelület AI javaslattal
- **Szerkeszthető promptok**: admin Prompts oldal — LLM promptok élőben szerkeszthetők, DB-ben tárolva
- **Változástörténet**: mezőszintű változáskövetés közösségeknél
- **DeepSeek elsődleges AI**: DeepSeek → Groq → Ollama fallback lánc
- **Magyar URL slug-ok**: összes nyilvános útvonal magyarosítva (`/felfedezes`, `/rolunk` stb.)
- **339 magyar helység**: az összes 1000+ fős magyarországi település hozzáadva
- **Személy és helyszín kinyerés**: pipeline személyeket és helyszíneket is kinyeri az oldalakból
- **Szitemap**: `/sitemap.xml` generált véletlenszerű frissítési listával

## 2026-05-08

- **Helyszín és személy adatmodellek**: `VenueRecord`, `PersonRecord` pydantic modellek + DB táblák
- **Admin helyszín/ember oldalak**: böngészhető lista az admin felületen
- **Keresési eredmény cache**: 7 napos TTL a keresési eredményekre városonként + témánként
- **Közösség oldal vizuális frissítés**: smaragd/pala paletta, kompaktabb elrendezés
- **Platformlink feliratok**: Facebook, Instagram, Meetup stb. azonosítva és megcímkézve
- **Előzmények és gyakoriság**: közösség részlet oldalán megjelenik, mikor és milyen rendszerességgel találkoznak
- **SQLite migráció**: teljes tárolás JSON fájlokból SQLite-ba migrálva
- **Hamis pozitív kezelés**: jelölés, prompt injektálás, verziókövetés

## 2026-05-07

- **Serper.dev keresés**: Serper → Brave → SearXNG fallback lánc
- **DeepSeek kinyerő**: DeepSeek → Groq → Ollama fallback lánc bevezetve
- **Dashboard futtatási presets**: 3 előre definiált konfiguráció + aktív szolgáltató sáv

## 2026-04-27

- **Groq fallback javítás**: rate limit már nem blokkolja a pipeline-t, azonnali Ollama fallback
- **Valós idejű feladatsor**: cache oldal AI gombokhoz prioritásos feladatsor
- **AI modell jelvény**: modell neve látható a cache sorokon, szűrhető

## 2026-04-26

- **Kiterjesztett közösség modell**: 11 új profilmező (helyszín, tagdíj, szint, stb.)
- **Hamis pozitív jelölés**: prompt injektálás + verziókövetés
- **Groq API kinyerő**: Groq → Ollama fallback, extract fingerprint (prompt+modell hash)
- **Eredmények tábla**: ország oszlop + oszloponkénti szűrők

## 2026-04-25

- **Publikus felfedező UI**: városok, témák, közösség kártyák `/` főoldalon
- **Egyéni közösség oldalak**: stabil ID-alapú URL-ek
- **i18n**: 50 nyelv, süti-alapú preferencia, téma feliratok fordítva
- **Tailwind CSS build**: Docker-be épített CSS, world map, about oldal
- **Brave Search API**: SearXNG mellé Brave mint keresési forrás
- **Visszajelzési form**: közösség oldalon e-mail küldéssel (Resend)
- **Fuzzy deduplikáció**: közel-azonos közösségek észlelése a store.py-ban

## 2026-04-24

- **Kezdeti projekt**: FastAPI admin felület, SQLite futástörténet, HTTP Basic Auth, kétszintű oldal cache, Ollama structured output (qwen2.5:7b)
