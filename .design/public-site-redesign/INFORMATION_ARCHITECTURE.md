# Information Architecture: kozossegek.com Public Site

## Site Map

- Home `/`
- Explore `/explore?city=&topic=`
  - City view `/explore?city={City}`
  - Topic view `/explore?topic={topic}`
  - City + Topic `/explore?city={City}&topic={topic}`
  - Tag view `/explore?tag={tag}`
- City shortcut `/{city-slug}` → redirects to `/explore?city={City}`
- City + Topic shortcut `/{city-slug}/{topic}` → resolves to explore or community detail
- Community Detail `/community/{community_id}` (also via `/{city-slug}/{community-slug}`)
- Source `/{url_hash}` — raw source page, low-traffic utility
- Map `/map`
- About `/about`
- Subscribe (POST) `/subscribe`
- Unsubscribe `/unsubscribe?token=`
- Language `/set-lang?lang=&next=`

Admin lives under `/admin/*` — out of scope for this redesign.

---

## Navigation Model

**Primary navigation** (sticky header, all viewports):

| Item | Destination | Active condition |
|------|------------|-----------------|
| kozossegek.com (logo) | `/` | always clickable |
| Discover | `/` or `/explore` | `/` and `/explore/*` |
| Map | `/map` | `/map` |
| About | `/about` | `/about` |
| Language switcher | `/set-lang` | utility, always right-aligned |

Maximum 3 labelled nav items + language. No hamburger on mobile — items are short enough to
scroll horizontally (current behavior, preserved).

**Secondary navigation** (contextual, within pages):

- *Home*: none — it IS the entry point
- *Explore*: sticky sub-bar with breadcrumb + "Change filters" link. Topic filter chips below
  the heading.
- *Community detail*: breadcrumb only (kozossegek.com / City / Topic / Name)
- *About*: no sub-nav needed

**Utility navigation**:

- Language switcher — right side of header, `<details>` dropdown
- Admin link — footer only, very low contrast (for operators, not visitors)

**Mobile navigation**:

No change to mechanism. Header stays sticky. Nav links scroll horizontally
(`overflow-x-auto`). Language switcher stays as a `<details>` dropdown at the far right.
On very narrow viewports (< 360px) "About" may be visually de-prioritised (lower opacity)
but remains reachable.

---

## Content Hierarchy

### Home `/`

1. **Search form** (city input + topic grid + Explore button) — the primary action; right
   panel, sticky on lg+. This is why people come.
2. **Headline + tagline** — establishes what the site is in 2 seconds.
3. **Stats strip** (communities, venues, people, cities, topics) — quick social proof.
4. **Popular cities grid** (user's country first) — browse entry point for people who haven't
   decided yet.
5. Footer — utility only.

### Explore `/explore`

1. **Page heading** (City, Topic, or "Communities worldwide") — orientation.
2. **Active topic chips** — let the user refine without going back to home.
3. **Community cards grid** — the payload; user spends most time here.
4. **Subscribe form** — secondary; appears below results.
5. Breadcrumb + "Change filters" — escape hatch, not primary.

### Community Detail `/community/{id}`

1. **Name + topic badge + city badge** — identity; must be obvious at a glance.
2. **Description** — the human pitch for the community.
3. **"Get involved" CTA zone** (website + primary social link) — the one action that matters.
4. **Detail sections** (location, schedule, contact, fee, members, level, etc.) — supporting
   facts, grouped logically.
5. **Additional links** (secondary social URLs) — for those who want more.
6. Breadcrumb — navigation context.

### About `/about`

1. **Mission statement** — why this exists; trust-building first.
2. **How it works** — transparency about data sourcing (AI + scraping).
3. **Stats** (cities, topics, communities) — proof of scale.
4. **Browse by interest** (topic chips) — conversion back into the product.
5. **Popular cities** — same conversion, geographic entry.
6. **Data quality notice** — honest caveat; builds more trust than hiding it.

### Map `/map`

1. **Map canvas** (full viewport minus header) — the entire point.
2. **Stats bar** (total communities, city count) — context strip above map.
3. City popups on click/hover — name, count, Explore button.

---

## User Flows

### Flow 1: "I just moved to a city, find me something" (primary)

1. User lands on `/`
2. Sees headline + search form
3. Types city name → topic counts update via `/api/city-topics`
4. Selects 1–2 topic chips
5. Clicks "Explore"
6. Arrives at `/explore?city={City}&topic={topic}`
7. Scans community cards
   - Finds something interesting → clicks card
   - Lands on `/community/{id}`
   - Sees description + CTA → clicks website/social link → **exits to community**
   - OR: subscribes for email updates → submits `/subscribe`
8. Doesn't find anything → clicks "Change filters" → back to `/` or `/explore?city={City}`

### Flow 2: "I'm interested in a topic, anywhere in the world"

1. User lands on `/` (or `/about`)
2. Clicks a topic chip without entering a city
3. Navigates directly to `/explore?topic={topic}`
4. Sees country-grouped, city-grouped results worldwide
5. Finds a city section → clicks community card → detail page → CTA

### Flow 3: "Show me what's near me on a map"

1. User clicks "Map" in nav
2. Lands on `/map`
3. Browses the world map, zooms to their region
4. Clicks a city marker → popup: city name + community count + "Explore" button
5. Clicks Explore → `/explore?city={City}`
6. Continues as Flow 1 from step 7

### Flow 4: "Someone sent me a link to a community"

1. User lands directly on `/community/{id}`
2. Reads name + description
3. Clicks primary CTA (website or social) → exits to community
   - OR clicks city badge → `/explore?city={City}` → discovers more
   - OR clicks topic badge → `/explore?city={City}&topic={topic}`

### Flow 5: "I want email updates for new communities in my city"

1. User is on `/explore?city={City}` (after Flow 1 step 6)
2. Scrolls past community cards
3. Sees subscribe form at bottom
4. Enters email → submits → POST `/subscribe`
5. Redirected back to `/explore?city={City}&subscribed=1`
6. Sees success banner at top of results

---

## Naming Conventions

| Concept | Label in UI | Notes |
|---|---|---|
| A scraped hobby/interest group | Community | Not "group", not "club", not "event" |
| The classification dimension | Topic | Not "category", not "tag" (tags are a separate secondary concept) |
| A tagged keyword on a community | Tag | Secondary; shown on detail page + filterable via `?tag=` |
| Geographic search dimension | City | Not "location", not "place" |
| The physical venue | Venue | Admin only; not shown on public site |
| The organiser / leader | Leader | Shown on detail page if available |
| Email update opt-in | Subscribe / Subscribers | Not "newsletter" |
| Source web page | Source | Admin only |

---

## Component Reuse Map

| Component | Used on | Behavior differences |
|---|---|---|
| `public_base.html` (header + footer) | All public pages | `lang` / `dir` varies per language; nav active state varies |
| Topic chip | Home (search form), Explore (filter), About (browse section) | Home: checkbox+label; Explore: form submit or JS filter; About: plain link |
| City card | Home (popular cities grid), About (popular cities) | Identical markup |
| Community card (full) | Explore grid | — |
| Community card (compact) | Explore worldwide grid (inside city sections) | Smaller, fewer fields shown |
| Stat block (number + label) | Home (strip), About (section) | Home: 5-col grid; About: 3-col grid |
| Subscribe form | Explore (bottom) | Single instance |
| Breadcrumb | Explore (sub-bar), Community detail | Explore: sticky sub-bar with filters; Detail: static inline |
| Detail row | Community detail | Grouped into sections in redesign |

---

## Content Growth Plan

**Communities** — grows continuously via scraper runs. The explore page handles this via
server-side pagination (currently all rendered; consider `?page=` for very large cities) and
client-side topic filtering. The worldwide topic view (`/explore?topic=X`) groups by country
then city — this pattern scales to hundreds of cities without structural change.

**Cities** — grows as new cities are added to config. The home page city grid shows top cities
by count; new cities appear automatically once they have data.

**Topics** — fixed set configured in `topics.yaml`. Additions require config change + new
translation keys. Not expected to grow frequently.

**Languages** — additions to `LANGUAGES` dict in `i18n.py` + new translation entries. UI
handles this automatically.

---

## URL Strategy

**Rules:**
- City slugs: lowercase, hyphenated, no accents (e.g., `budapest`, `new-york`, `sao-paulo`)
- Topic values: underscore-separated snake_case (e.g., `board_games`, `language_exchange`) —
  matches internal topic keys
- Community IDs: opaque numeric or UUID, not semantic slugs (current: `/{city-slug}/{segment}`
  resolves via DB lookup)
- Tags: raw string, URL-encoded in query param (`?tag=meetup`)

**Dynamic segments:**
- `/{city_slug}` — resolved to city name via DB, then redirects or renders explore
- `/{city_slug}/{segment}` — segment is either a topic key or a community identifier
- `/community/{community_id}` — canonical community URL

**Query parameters:**
- `city` — city name (free text, matched against DB)
- `topic` — topic key (snake_case)
- `tag` — tag string
- `subscribed` — `1` to show success banner post-subscribe
- `lang` — language code (on `/set-lang` only)
- `next` — redirect target (on `/set-lang` only)
- `token` — unsubscribe token (on `/unsubscribe` only)

**Avoid:**
- Encoding city names in the path for explore (use `?city=` query param — already the case)
- Multiple topic values in path (use repeated `?topic=` params — already the case)
