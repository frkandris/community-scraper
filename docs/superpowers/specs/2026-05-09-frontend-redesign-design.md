# Design: Frontend redesign — terrakotta gradient hero

**Date:** 2026-05-09

## Összefoglaló

A jelenlegi "meleg editoriális minimalizmus" design megmarad, de vizuálisan erőteljesebb, karakteresebb lesz. A terrakotta márkacsín az egész publikus felületen következetesen megjelenik: a főoldalon teljes gradient hero szekcióként, minden más publikus oldalon kisebb fejléc gradient sávként.

## Design döntések

| Kérdés | Döntés |
|--------|--------|
| Aesthetic irány | Terrakotta gradiens hero (meleg, organikus) |
| Hatókör | Minden publikus oldal |
| Sticky nav header | Marad fehér |
| Másodlagos oldalak | Mini gradient fejléc sáv |

## Gradient

```
background: linear-gradient(135deg, #8A4226 0%, #A8512F 30%, #C2613A 65%, #E88E6B 100%)
```

Dekoratív átlátszó körök a gradiens hátterén (position:absolute, opacity 5–8%), hogy organikusabb legyen.

## Változtatások oldalanként

### `public_base.html` — header/footer
- Header: marad fehér, változatlan
- Footer: nincs változtatás

### `public_home.html` — főoldal
A jelenlegi `<div class="max-w-6xl ...">` gridben a bal oldal teteje (h1 + subtitle) helyett egy teljes szélességű gradient hero szekció kerül:
- Gradient háttér dekoratív körökkel
- Fehér szöveg: kis uppercase overline ("Magyar közösségek"), nagy h1 (`font-size: ~2.25rem`, `font-weight: 900`, `letter-spacing: -0.03em`), fehér subtitle
- A hero szekción kívüli tartalom (stats strip, városlista, search form) változatlan marad

### Minden más publikus oldal (explore, community detail, cities, map, about, venues, people)
Az oldal tetején (a sticky sub-bar alatt, vagy ahol az oldal főcíme volt) egy kisebb gradient sáv:
- Ugyanaz a gradient, de alacsonyabb magasság (~60–80px padding)
- Fehér overline (oldal/kontextus info, pl. "Budapest · Futás"), fehér h1 (az oldal főcíme/száma), fehér subtitle (rövid leírás)
- Dekoratív kör (csak egy, jobb sarokban)

### Kártyák (explore, community list)
- `box-shadow` növelése: `0 2px 8px rgba(28,25,23,0.06)` az összes community kártyán
- Border megmarad (`border: 1.5px solid #EAE5DB`)
- Hover state erősítése: `hover:shadow-md hover:border-[#E88E6B]`

## Érintett fájlok

| Fájl | Változás |
|------|---------|
| `scraper/web/templates/public_home.html` | Gradient hero szekció a h1/subtitle helyett |
| `scraper/web/templates/public_explore.html` | Mini gradient sáv az oldal tetején |
| `scraper/web/templates/public_community.html` | Mini gradient sáv az oldal tetején |
| `scraper/web/templates/public_cities.html` | Mini gradient sáv |
| `scraper/web/templates/public_map.html` | Mini gradient sáv |
| `scraper/web/templates/public_about.html` | Mini gradient sáv |
| `scraper/web/templates/public_venues.html` | Mini gradient sáv |
| `scraper/web/templates/public_people.html` | Mini gradient sáv |
| `scraper/web/static/css/input.css` | Esetleg gradient utility class hozzáadása |

## Nem változik

- A nav header (fehér marad)
- Az admin felület (`base.html`, dashboard stb.)
- Az i18n/backend logika
- A search form és a topic chip-ek vizuális stílusa
- A color tokens (`input.css` `:root` szekció)
