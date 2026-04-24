# Community Scraper

Városok × közösségi témák alapján keresi az interneten aktív közösségeket, lokális LLM-mel értelmezi az eredményeket, és strukturált JSON fájlokba menti őket.

## Architektúra

```
[scraper app :8001] → [SearXNG :8080]   # keresés
[scraper app :8001] → [Ollama  :11434]  # lokális LLM
```

## Komponensek

- **SearXNG** – self-hosted metakereső
- **Ollama** – lokális LLM (llama3.2:3b)
- **FastAPI** – web admin felület (`:8001`)
- **APScheduler** – ütemezett futtatás (alapértelmezés: naponta 3:00 UTC)

## Web felület

| Oldal | URL | Leírás |
|-------|-----|--------|
| Dashboard | `/` | Státusz, manuális indítás |
| Results | `/results` | Eredmények városonként/témánként |
| Config | `/config` | YAML konfiguráció szerkesztő |
| Logs | `/logs` | Valós idejű napló (SSE) |
| History | `/history` | Git commit history |

## Konfiguráció

`config/cities.yaml` – városok és locale-juk  
`config/topics.yaml` – témák és keresési kifejezések locale-onként  
`config/settings.yaml` – Ollama/SearXNG/fetch paraméterek

## Env változók

| Változó | Alapértelmezés | Leírás |
|---------|---------------|--------|
| `SEARXNG_URL` | `http://localhost:8080` | SearXNG alap URL |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama alap URL |
| `SCHEDULE_CRON` | `0 3 * * *` | Futási ütemezés (cron) |
| `GIT_USER_NAME` | – | Git commit szerző neve |
| `GIT_USER_EMAIL` | – | Git commit szerző e-mailje |
| `GIT_TOKEN` | – | GitHub PAT (opcionális push-hoz) |

## Adatok

Az eredmények `data/<city>/<topic>/communities.json` fájlokban tárolódnak, minden futás után auto-git-commit rögzíti a változásokat.
