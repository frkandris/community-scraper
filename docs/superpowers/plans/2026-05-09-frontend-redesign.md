# Frontend Redesign — Terrakotta Gradient Hero Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a full-bleed terrakotta gradient hero to the homepage and compact gradient header strips to all other public-facing pages.

**Architecture:** Template-only changes (8 HTML files + 2 CSS files). A `.brand-gradient` CSS utility class is added to `app.css` and `input.css` once, then used in every template — no repeated inline gradient strings. The homepage hero is a new full-bleed section inserted before the existing grid. Other pages get a mini strip inserted before (outside) their `max-w-*` content wrapper.

**Tech Stack:** Jinja2 templates, Tailwind CSS (CDN + compiled `app.css`), custom CSS component class.

---

## Task 1: Add `.brand-gradient` CSS class

**Files:**
- Modify: `scraper/web/static/css/app.css`
- Modify: `scraper/web/static/css/input.css`

- [ ] **Step 1: Add class to `app.css`**

Append to the end of `scraper/web/static/css/app.css` (after the `.chip-active` block):

```css
/* Brand gradient — terrakotta hero/strip background */
.brand-gradient {
  background: linear-gradient(135deg, #8A4226 0%, #A8512F 30%, #C2613A 65%, #E88E6B 100%);
}
```

- [ ] **Step 2: Add same class to `input.css` for future rebuilds**

Append the same block to the end of `scraper/web/static/css/input.css` (after `.chip-active`), inside the `@layer components { }` block.

- [ ] **Step 3: Verify server serves the class**

```bash
ADMIN_PASSWORD=test python -m scraper.main &
sleep 3
curl -s http://localhost:8001/static/css/app.css | grep brand-gradient
```

Expected output: `/* Brand gradient` line appears.

- [ ] **Step 4: Kill the dev server**

```bash
pkill -f "scraper.main"
```

- [ ] **Step 5: Commit**

```bash
git add scraper/web/static/css/app.css scraper/web/static/css/input.css
git commit -m "style: add brand-gradient utility class"
```

---

## Task 2: Homepage gradient hero (`public_home.html`)

**Files:**
- Modify: `scraper/web/templates/public_home.html`

The current page opens with `<div class="max-w-6xl mx-auto px-4 sm:px-6 py-8 sm:py-12">` containing a 2-column grid. The left column's first child is `<div class="border-b border-[#EAE5DB] pb-8">` with the h1 and subtitle. 

Plan: move the h1+subtitle into a new full-bleed gradient hero **before** the grid, and remove the now-empty `border-b` wrapper from the grid.

- [ ] **Step 1: Insert the gradient hero before the outer container**

In `public_home.html`, replace:

```html
<div class="max-w-6xl mx-auto px-4 sm:px-6 py-8 sm:py-12">
  <div class="grid grid-cols-1 lg:grid-cols-[minmax(0,1.05fr)_minmax(340px,0.95fr)] gap-8 items-start">

    <!-- Left: hero + stats + cities -->
    <section class="space-y-8">
      <div class="border-b border-[#EAE5DB] pb-8">
        <h1 class="text-3xl sm:text-4xl font-bold tracking-tight text-[#1C1917] leading-tight">
          {{ t('home_title') }}
        </h1>
        <p class="mt-4 text-base text-[#6A6259] leading-relaxed max-w-2xl">
          {{ t('home_subtitle') }}
        </p>
      </div>
```

with:

```html
<!-- Gradient hero — full bleed -->
<div class="brand-gradient relative overflow-hidden">
  <div class="absolute -top-12 -right-10 w-52 h-52 rounded-full bg-white opacity-[0.07] pointer-events-none"></div>
  <div class="absolute -bottom-10 -left-6 w-36 h-36 rounded-full bg-white opacity-[0.05] pointer-events-none"></div>
  <div class="absolute top-6 right-[18%] w-24 h-24 rounded-full bg-white opacity-[0.06] pointer-events-none"></div>
  <div class="max-w-6xl mx-auto px-4 sm:px-6 py-10 sm:py-14 relative">
    <p class="text-xs font-semibold uppercase tracking-[0.12em] text-white/65 mb-3">Magyar közösségek</p>
    <h1 class="text-3xl sm:text-4xl font-black tracking-[-0.03em] text-white leading-[1.1] mb-4">
      {{ t('home_title') }}
    </h1>
    <p class="text-base text-white/80 leading-relaxed max-w-lg">
      {{ t('home_subtitle') }}
    </p>
  </div>
</div>

<div class="max-w-6xl mx-auto px-4 sm:px-6 pt-8 pb-12">
  <div class="grid grid-cols-1 lg:grid-cols-[minmax(0,1.05fr)_minmax(340px,0.95fr)] gap-8 items-start">

    <!-- Left: stats + cities -->
    <section class="space-y-8">
```

- [ ] **Step 2: Verify in browser**

```bash
ADMIN_PASSWORD=test python -m scraper.main &
sleep 3
```

Open http://localhost:8001 — gradient hero should appear above the stats strip. Text should be white on terrakotta gradient with decorative circles.

- [ ] **Step 3: Kill dev server and commit**

```bash
pkill -f "scraper.main"
git add scraper/web/templates/public_home.html
git commit -m "feat: terrakotta gradient hero on homepage"
```

---

## Task 3: Explore page gradient strips (`public_explore.html`)

**Files:**
- Modify: `scraper/web/templates/public_explore.html`

The explore page has 3 content states after the sticky sub-bar. Each gets its own gradient strip. The strip goes **outside** the `<div class="max-w-6xl mx-auto px-4 sm:px-6 py-8">` wrapper, so it bleeds full-width.

- [ ] **Step 1: Restructure — move gradient strips outside max-w-6xl**

In `public_explore.html`, replace the opening of the main content block:

```html
<div class="max-w-6xl mx-auto px-4 sm:px-6 py-8">

  {% if subscribed %}
  ...
  {% endif %}

  <!-- Tag search results -->
  {% if tag %}
  <div class="mb-8 border-b border-[#EAE5DB] pb-5">
    <h1 class="text-2xl font-bold text-[#1C1917] mb-1 flex items-center gap-2">
      <i class="ph ph-tag text-[#C2613A]"></i> {{ tag }}
    </h1>
    <p class="text-sm text-[#8C8478] mt-1">{{ tag_records | length }} közösség ezzel a témával{% if city %} – {{ city }}{% endif %}</p>
  </div>

  {% if tag_records %}
```

with:

```html
<!-- Gradient strips — one per content state, full-bleed -->
{% if tag %}
<div class="brand-gradient relative overflow-hidden">
  <div class="absolute -top-6 -right-6 w-28 h-28 rounded-full bg-white opacity-[0.07] pointer-events-none"></div>
  <div class="max-w-6xl mx-auto px-4 sm:px-6 py-5 sm:py-7 relative">
    <p class="text-[10px] font-semibold uppercase tracking-[0.1em] text-white/70 mb-1.5">
      <i class="ph ph-tag"></i> Tag
    </p>
    <h1 class="text-xl sm:text-2xl font-black tracking-[-0.025em] text-white leading-tight">{{ tag }}</h1>
    <p class="text-sm text-white/75 mt-1">{{ tag_records | length }} közösség{% if city %} – {{ city }}{% endif %}</p>
  </div>
</div>
{% elif country_sections %}
<div class="brand-gradient relative overflow-hidden">
  <div class="absolute -top-6 -right-6 w-28 h-28 rounded-full bg-white opacity-[0.07] pointer-events-none"></div>
  <div class="max-w-6xl mx-auto px-4 sm:px-6 py-5 sm:py-7 relative">
    <p class="text-[10px] font-semibold uppercase tracking-[0.1em] text-white/70 mb-1.5">{{ t('nav_discover') }}</p>
    <h1 class="text-xl sm:text-2xl font-black tracking-[-0.025em] text-white leading-tight">
      {% if selected_topics | length == 1 %}
        {{ topic_labels.get(selected_topics[0], selected_topics[0].replace('_',' ').title()) }}
      {% else %}
        {{ t('explore_communities_worldwide') }}
      {% endif %}
    </h1>
  </div>
</div>
{% elif city %}
<div class="brand-gradient relative overflow-hidden">
  <div class="absolute -top-6 -right-6 w-28 h-28 rounded-full bg-white opacity-[0.07] pointer-events-none"></div>
  <div class="max-w-6xl mx-auto px-4 sm:px-6 py-5 sm:py-7 relative">
    <p class="text-[10px] font-semibold uppercase tracking-[0.1em] text-white/70 mb-1.5">{{ city }}</p>
    <h1 class="text-xl sm:text-2xl font-black tracking-[-0.025em] text-white leading-tight">
      {% if selected_topics | length == 1 %}
        {{ t('explore_topic_in', label=topic_labels.get(selected_topics[0], selected_topics[0].replace('_',' ').title()), city=city) }}
      {% else %}
        {{ t('explore_communities_in', city=city) }}
      {% endif %}
    </h1>
    {% if total > 0 %}
    <p class="text-sm text-white/75 mt-1" id="visible-count" data-tpl="{{ t('explore_n_communities', n='COUNT') }}">{{ t('explore_n_communities', n=total) }}</p>
    {% endif %}
  </div>
</div>
{% endif %}

<div class="max-w-6xl mx-auto px-4 sm:px-6 py-6">

  {% if subscribed %}
  ...
  {% endif %}

  <!-- Tag search results -->
  {% if tag %}

  {% if tag_records %}
```

- [ ] **Step 2: Remove the old header divs for tag and country_sections states**

Inside the `max-w-6xl` div, the old tag header div (lines 55-60) is now replaced by the external strip. Delete:

```html
  <div class="mb-8 border-b border-[#EAE5DB] pb-5">
    <h1 class="text-2xl font-bold text-[#1C1917] mb-1 flex items-center gap-2">
      <i class="ph ph-tag text-[#C2613A]"></i> {{ tag }}
    </h1>
    <p class="text-sm text-[#8C8478] mt-1">{{ tag_records | length }} közösség ezzel a témával{% if city %} – {{ city }}{% endif %}</p>
  </div>
```

And delete the country_sections header div (lines 91-103):

```html
  <div class="mb-8 border-b border-[#EAE5DB] pb-5">
    <h1 class="text-2xl font-bold text-[#1C1917] mb-1">
      {% if selected_topics | length == 1 %}
        <i class="ph ph-{{ topic_icons.get(selected_topics[0], 'circle') }} text-[#C2613A]"></i>
        {{ topic_labels.get(selected_topics[0], selected_topics[0].replace('_',' ').title()) }} – {{ t('explore_communities_worldwide') }}
      {% else %}
        {{ t('explore_communities_worldwide') }}
      {% endif %}
    </h1>
    <p class="text-sm text-[#8C8478] mt-1">
      {{ t('explore_showing_all_cities', link='<a href="/" class="text-[#A8512F] hover:underline">' ~ t('explore_pick_city') ~ '</a>') | safe }}
    </p>
  </div>
```

- [ ] **Step 3: Restructure the city-results header (lines 154-167)**

Replace:

```html
  <div class="flex items-start justify-between mb-6 flex-wrap gap-4 border-b border-[#EAE5DB] pb-5">
    <div>
      <h1 class="text-2xl font-bold text-[#1C1917]">
        {% if selected_topics | length == 1 %}
          <i class="ph ph-{{ topic_icons.get(selected_topics[0], 'circle') }} text-[#C2613A]"></i>
          {{ t('explore_topic_in', label=topic_labels.get(selected_topics[0], selected_topics[0].replace('_',' ').title()), city=city) }}
        {% else %}
          {{ t('explore_communities_in', city=city) }}
        {% endif %}
      </h1>
      {% if total > 0 %}
      <p class="text-sm text-[#8C8478] mt-0.5" id="visible-count" data-tpl="{{ t('explore_n_communities', n='COUNT') }}">{{ t('explore_n_communities', n=total) }}</p>
      {% endif %}
    </div>
```

with (just the chips wrapper, the h1 now lives in the gradient strip):

```html
  <div class="flex items-center justify-end mb-6 flex-wrap gap-2 border-b border-[#EAE5DB] pb-4">
```

Note: the `id="visible-count"` paragraph and the `data-tpl` attribute have moved to the gradient strip — verify the JS in the template still works. Search for any `document.getElementById('visible-count')` usage in the file.

- [ ] **Step 4: Check `visible-count` JS references**

```bash
grep -n "visible-count" scraper/web/templates/public_explore.html
```

If the JS reads `visible-count` to update the count live (via `data-tpl`), the element is now in the gradient strip (outside `max-w-6xl`). The JS only manipulates `textContent`, so the location doesn't matter — it will still work.

- [ ] **Step 5: Verify in browser**

```bash
ADMIN_PASSWORD=test python -m scraper.main &
sleep 3
```

Open http://localhost:8001/felfedezes?city=Budapest — mini gradient strip above results. Open http://localhost:8001/felfedezes?city=Budapest&topic=futás — strip shows topic name.

- [ ] **Step 6: Kill dev server and commit**

```bash
pkill -f "scraper.main"
git add scraper/web/templates/public_explore.html
git commit -m "feat: gradient strips on explore page"
```

---

## Task 4: Community detail gradient strip (`public_community.html`)

**Files:**
- Modify: `scraper/web/templates/public_community.html`

The current page opens at line 7 with `<div class="max-w-3xl mx-auto px-4 sm:px-6 py-8 sm:py-10">`. Insert the mini gradient strip before this wrapper.

- [ ] **Step 1: Insert gradient strip before the outer wrapper**

In `public_community.html`, replace:

```html
{% block content %}

<div class="max-w-3xl mx-auto px-4 sm:px-6 py-8 sm:py-10">

  <!-- Breadcrumb -->
  <nav class="flex items-center gap-1.5 text-sm text-[#8C8478] mb-6 overflow-hidden flex-wrap">
```

with:

```html
{% block content %}

<!-- Mini gradient strip -->
<div class="brand-gradient relative overflow-hidden">
  <div class="absolute -top-6 -right-6 w-28 h-28 rounded-full bg-white opacity-[0.07] pointer-events-none"></div>
  <div class="max-w-3xl mx-auto px-4 sm:px-6 py-5 sm:py-7 relative">
    <p class="text-[10px] font-semibold uppercase tracking-[0.1em] text-white/70 mb-1.5">
      {{ city }}{% if topic %} · {{ topic_labels.get(topic, topic.replace('_',' ').title()) }}{% endif %}
    </p>
    <h1 class="text-xl sm:text-2xl font-black tracking-[-0.025em] text-white leading-tight">{{ r.name }}</h1>
  </div>
</div>

<div class="max-w-3xl mx-auto px-4 sm:px-6 py-6 sm:py-8">

  <!-- Breadcrumb -->
  <nav class="flex items-center gap-1.5 text-sm text-[#8C8478] mb-6 overflow-hidden flex-wrap">
```

- [ ] **Step 2: Remove the `<h1>` from the existing header card** (it's now in the gradient strip)

In the header card section (around line 36), change:

```html
        <h1 class="text-2xl font-bold text-[#1C1917] leading-tight">{{ r.name }}</h1>
```

to:

```html
        <p class="text-lg font-bold text-[#1C1917] leading-tight">{{ r.name }}</p>
```

This avoids duplicate h1 on the page (the gradient strip now has the h1).

- [ ] **Step 3: Verify in browser**

Open any community detail URL, e.g. http://localhost:8001/budapest/futás — the gradient strip should show city·topic and community name. The header card below still shows the icon, chips, and description.

- [ ] **Step 4: Commit**

```bash
pkill -f "scraper.main"
git add scraper/web/templates/public_community.html
git commit -m "feat: gradient strip on community detail page"
```

---

## Task 5: Remaining pages — cities, about, venues, people

**Files:**
- Modify: `scraper/web/templates/public_cities.html`
- Modify: `scraper/web/templates/public_about.html`
- Modify: `scraper/web/templates/public_venues.html`
- Modify: `scraper/web/templates/public_people.html`

All follow the same pattern: insert a mini gradient strip before the page's main wrapper, then remove the now-redundant inner header div.

### 5a — Cities page

- [ ] **Step 1: Add gradient strip to `public_cities.html`**

Replace:

```html
{% block content %}

<div class="max-w-6xl mx-auto px-4 sm:px-6 py-8">

  <div class="mb-8 border-b border-[#EAE5DB] pb-6">
    <p class="text-xs font-semibold uppercase tracking-[0.08em] text-[#A8512F] mb-2">{{ t('nav_discover') }}</p>
    <h1 class="text-2xl sm:text-3xl font-bold text-[#1C1917]">{{ t('cities_title') }}</h1>
    <p class="text-sm text-[#8C8478] mt-2">{{ total_cities }} magyar város</p>
  </div>
```

with:

```html
{% block content %}

<div class="brand-gradient relative overflow-hidden">
  <div class="absolute -top-6 -right-6 w-28 h-28 rounded-full bg-white opacity-[0.07] pointer-events-none"></div>
  <div class="max-w-6xl mx-auto px-4 sm:px-6 py-5 sm:py-7 relative">
    <p class="text-[10px] font-semibold uppercase tracking-[0.1em] text-white/70 mb-1.5">{{ t('nav_discover') }}</p>
    <h1 class="text-xl sm:text-2xl font-black tracking-[-0.025em] text-white leading-tight">{{ t('cities_title') }}</h1>
    <p class="text-sm text-white/75 mt-1">{{ total_cities }} magyar város</p>
  </div>
</div>

<div class="max-w-6xl mx-auto px-4 sm:px-6 py-6">
```

### 5b — About page

- [ ] **Step 2: Add gradient strip to `public_about.html`**

Replace:

```html
{% block content %}

<div class="max-w-6xl mx-auto px-4 sm:px-6 py-10 space-y-12">

  <!-- Mission: trust-first -->
  <div class="max-w-3xl border-b border-[#EAE5DB] pb-10">
    <p class="text-xs font-semibold uppercase tracking-[0.08em] text-[#A8512F] mb-3">{{ t('nav_about') }}</p>
    <h1 class="text-3xl font-bold tracking-tight text-[#1C1917] mb-5">{{ t('about_title') }}</h1>
    <div class="space-y-4 text-[#6A6259] leading-relaxed">
      <p>{{ t('about_description') }}</p>
    </div>
  </div>
```

with:

```html
{% block content %}

<div class="brand-gradient relative overflow-hidden">
  <div class="absolute -top-6 -right-6 w-28 h-28 rounded-full bg-white opacity-[0.07] pointer-events-none"></div>
  <div class="max-w-6xl mx-auto px-4 sm:px-6 py-5 sm:py-7 relative">
    <p class="text-[10px] font-semibold uppercase tracking-[0.1em] text-white/70 mb-1.5">közösségek.com</p>
    <h1 class="text-xl sm:text-2xl font-black tracking-[-0.025em] text-white leading-tight">{{ t('about_title') }}</h1>
  </div>
</div>

<div class="max-w-6xl mx-auto px-4 sm:px-6 py-10 space-y-12">

  <!-- Mission: trust-first -->
  <div class="max-w-3xl border-b border-[#EAE5DB] pb-10">
    <div class="space-y-4 text-[#6A6259] leading-relaxed">
      <p>{{ t('about_description') }}</p>
    </div>
  </div>
```

### 5c — Venues page

- [ ] **Step 3: Add gradient strip to `public_venues.html`**

Replace (line 58-66):

```html
<div class="max-w-6xl mx-auto px-4 sm:px-6 py-8">

  <div class="mb-6 border-b border-[#EAE5DB] pb-5">
    <p class="text-xs font-semibold uppercase tracking-[0.08em] text-[#A8512F] mb-2">Helyszínek</p>
    <h1 class="text-2xl sm:text-3xl font-bold text-[#1C1917]">Közösségeknek otthont adó helyszínek</h1>
    <p class="text-sm text-[#8C8478] mt-2">
      {{ venues | length }}{% if selected_city or selected_topic %} / {{ total_all }}{% endif %} helyszín
    </p>
  </div>
```

with:

```html
<div class="brand-gradient relative overflow-hidden">
  <div class="absolute -top-6 -right-6 w-28 h-28 rounded-full bg-white opacity-[0.07] pointer-events-none"></div>
  <div class="max-w-6xl mx-auto px-4 sm:px-6 py-5 sm:py-7 relative">
    <p class="text-[10px] font-semibold uppercase tracking-[0.1em] text-white/70 mb-1.5">Helyszínek</p>
    <h1 class="text-xl sm:text-2xl font-black tracking-[-0.025em] text-white leading-tight">Közösségeknek otthont adó helyszínek</h1>
    <p class="text-sm text-white/75 mt-1">{{ venues | length }}{% if selected_city or selected_topic %} / {{ total_all }}{% endif %} helyszín</p>
  </div>
</div>

<div class="max-w-6xl mx-auto px-4 sm:px-6 py-6">
```

### 5d — People page

- [ ] **Step 4: Redesign `public_people.html` with gradient strip**

The current people page is a centered empty-state layout. Replace the entire `{% block content %}` with:

```html
{% block content %}

<div class="brand-gradient relative overflow-hidden">
  <div class="absolute -top-6 -right-6 w-28 h-28 rounded-full bg-white opacity-[0.07] pointer-events-none"></div>
  <div class="max-w-6xl mx-auto px-4 sm:px-6 py-5 sm:py-7 relative">
    <p class="text-[10px] font-semibold uppercase tracking-[0.1em] text-white/70 mb-1.5">Közösségi emberek</p>
    <h1 class="text-xl sm:text-2xl font-black tracking-[-0.025em] text-white leading-tight">Közösségi vezetők és oktatók</h1>
    <p class="text-sm text-white/75 mt-1">{{ total_persons }} személy az adatbázisban</p>
  </div>
</div>

<div class="max-w-2xl mx-auto px-4 sm:px-6 py-12 text-center">
  <div class="inline-flex h-16 w-16 items-center justify-center rounded-full bg-[#F5F2EC] border border-[#EAE5DB] mb-6">
    <i class="ph ph-users text-3xl text-[#A8512F]"></i>
  </div>
  <p class="text-sm text-[#8C8478] mb-6">Közösségek vezetői, oktatók, előadók</p>
  <div class="mt-6">
    <a href="/"
       class="inline-block px-6 py-2.5 bg-[#A8512F] text-white text-sm font-semibold rounded-lg hover:bg-[#8A4226] transition-colors">
      Közösségek felfedezése →
    </a>
  </div>
</div>

{% endblock %}
```

- [ ] **Step 5: Verify all 4 pages in browser**

```bash
ADMIN_PASSWORD=test python -m scraper.main &
sleep 3
```

Check:
- http://localhost:8001/varosok — cities gradient strip
- http://localhost:8001/rolunk — about gradient strip
- http://localhost:8001/helyszinek — venues gradient strip
- http://localhost:8001/emberek — people gradient strip

- [ ] **Step 6: Commit**

```bash
pkill -f "scraper.main"
git add scraper/web/templates/public_cities.html scraper/web/templates/public_about.html scraper/web/templates/public_venues.html scraper/web/templates/public_people.html
git commit -m "feat: gradient strips on cities, about, venues, people pages"
```

---

## Task 6: Map page gradient strip (`public_map.html`)

**Files:**
- Modify: `scraper/web/templates/public_map.html`

The map page has a compact stats bar (not a tall page header) and then a full-viewport `#map`. A full mini strip would push the map down significantly. Instead: replace the existing stats bar background with the brand gradient.

- [ ] **Step 1: Apply gradient to the existing stats bar**

In `public_map.html`, replace:

```html
<!-- Stats bar -->
<div class="bg-white border-b border-[#EAE5DB]">
  <div class="max-w-6xl mx-auto px-4 sm:px-6 py-3 flex items-center gap-6 flex-wrap">
    <h1 class="text-sm font-semibold text-[#1C1917] flex items-center gap-2">
      <i class="ph ph-map-trifold text-[#C2613A]"></i>
      {{ t('map_title') }}
    </h1>
```

with:

```html
<!-- Stats bar -->
<div class="brand-gradient">
  <div class="max-w-6xl mx-auto px-4 sm:px-6 py-3 flex items-center gap-6 flex-wrap">
    <h1 class="text-sm font-semibold text-white flex items-center gap-2">
      <i class="ph ph-map-trifold text-white/80"></i>
      {{ t('map_title') }}
    </h1>
```

- [ ] **Step 2: Update stat text colours inside the stats bar to white**

Replace the full inner stats bar content:

```html
    <h1 class="text-sm font-semibold text-[#1C1917] flex items-center gap-2">
      <i class="ph ph-map-trifold text-[#C2613A]"></i>
      {{ t('map_title') }}
    </h1>
    <div class="flex items-center gap-1.5 text-xs text-[#8C8478] ml-auto">
      <span class="inline-block w-3 h-3 rounded-full bg-[#C2613A] opacity-80 shrink-0"></span>
      <strong class="text-[#1C1917]">{{ total }}</strong> {{ t('map_n_communities') }}
      {{ t('map_n_in') }}
      <strong class="text-[#1C1917]">{{ cities_with_data }}</strong> {{ t('map_n_city') if cities_with_data == 1 else t('map_n_cities') }}
    </div>
```

with:

```html
    <h1 class="text-sm font-semibold text-white flex items-center gap-2">
      <i class="ph ph-map-trifold text-white/80"></i>
      {{ t('map_title') }}
    </h1>
    <div class="flex items-center gap-1.5 text-xs text-white/70 ml-auto">
      <span class="inline-block w-3 h-3 rounded-full bg-white/80 shrink-0"></span>
      <strong class="text-white">{{ total }}</strong> {{ t('map_n_communities') }}
      {{ t('map_n_in') }}
      <strong class="text-white">{{ cities_with_data }}</strong> {{ t('map_n_city') if cities_with_data == 1 else t('map_n_cities') }}
    </div>
```

- [ ] **Step 3: Verify**

```bash
ADMIN_PASSWORD=test python -m scraper.main &
sleep 3
```

Open http://localhost:8001/terkep — stats bar should be terrakotta gradient, map below unchanged.

- [ ] **Step 4: Commit**

```bash
pkill -f "scraper.main"
git add scraper/web/templates/public_map.html
git commit -m "feat: gradient stats bar on map page"
```

---

## Task 7: Improve card shadows on explore page

**Files:**
- Modify: `scraper/web/templates/public_explore.html`

Community cards currently use `border border-[#EAE5DB]` with no shadow. Add `shadow-sm` and a stronger hover shadow.

- [ ] **Step 1: Update tag search result cards (lines ~65)**

Find:

```html
    <a href="{{ r.community_url }}" class="block bg-white rounded-lg border border-[#EAE5DB] p-4 flex flex-col gap-2 hover:border-[#E88E6B] hover:bg-[#FDF0EA]/40 transition-colors">
```

Replace with:

```html
    <a href="{{ r.community_url }}" class="block bg-white rounded-lg border border-[#EAE5DB] p-4 flex flex-col gap-2 shadow-sm hover:border-[#E88E6B] hover:bg-[#FDF0EA]/40 hover:shadow-md transition-all">
```

- [ ] **Step 2: Update country-section cards (lines ~122)**

Find:

```html
        <a href="{{ r.community_url }}" class="block bg-white rounded-lg border border-[#EAE5DB] p-3 flex flex-col gap-1.5 hover:border-[#E88E6B] hover:bg-[#FDF0EA]/40 transition-colors">
```

Replace with:

```html
        <a href="{{ r.community_url }}" class="block bg-white rounded-lg border border-[#EAE5DB] p-3 flex flex-col gap-1.5 shadow-sm hover:border-[#E88E6B] hover:bg-[#FDF0EA]/40 hover:shadow-md transition-all">
```

- [ ] **Step 3: Verify**

```bash
ADMIN_PASSWORD=test python -m scraper.main &
sleep 3
```

Open http://localhost:8001/felfedezes?city=Budapest — cards should have visible shadow and stronger hover effect.

- [ ] **Step 4: Commit**

```bash
pkill -f "scraper.main"
git add scraper/web/templates/public_explore.html
git commit -m "style: deeper card shadows on explore page"
```

---

## Task 8: Final verification pass

- [ ] **Step 1: Start server**

```bash
ADMIN_PASSWORD=test python -m scraper.main &
sleep 3
```

- [ ] **Step 2: Check all public pages**

| URL | Expected |
|-----|----------|
| http://localhost:8001/ | Full-bleed terrakotta gradient hero with h1 + subtitle |
| http://localhost:8001/felfedezes?city=Budapest | Mini gradient strip above results |
| http://localhost:8001/felfedezes?city=Budapest&topic=futás | Strip shows topic name |
| http://localhost:8001/varosok | Mini gradient strip |
| http://localhost:8001/rolunk | Mini gradient strip |
| http://localhost:8001/helyszinek | Mini gradient strip |
| http://localhost:8001/emberek | Mini gradient strip |
| http://localhost:8001/terkep | Gradient stats bar, map unchanged |

- [ ] **Step 3: Check admin pages are unaffected**

Open http://localhost:8001/admin — admin UI should look completely unchanged (uses `base.html`, not `public_base.html`).

- [ ] **Step 4: Kill server**

```bash
pkill -f "scraper.main"
```

- [ ] **Step 5: Final commit (if any remaining changes)**

```bash
git status
# commit anything unstaged
```
