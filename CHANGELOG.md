# Changelog

## 2026-05-15

- **Multi-domain support**: `közösségek.com` (HU cities, HU UI) and `meetapedia.com` (all cities, EN UI) served from one container
- `_detect_site(request)` in `i18n.py` — Host-header-based domain detection
- `_site_cities(request)` in `app.py` — per-domain city filtering (HU-only vs. all)
- Site-aware home stats cache keyed by domain (`dict[str, dict]`)
- All public templates updated to use `{{ site_name }}`, `{{ site_url }}`, `{{ lang }}`, `{{ locale }}` variables
- International cities pipeline run added in scheduler and startup after HU run

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
