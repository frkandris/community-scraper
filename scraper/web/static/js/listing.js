/* Shared public-site UI helpers.
 *
 * Two independent widgets, both built around the same accent-insensitive
 * matcher so "szentendre", "Szentendré" and "SZENTENDRE" all find Szentendre:
 *
 *   MpText.norm(s)          — NFD-fold to lowercase ASCII-ish for comparison
 *   MpAutocomplete.attach() — touch-friendly city suggestion dropdown
 *   MpListFilter.attach()   — A-Z jump bar + free-text filter over a rendered list
 *
 * Why not <datalist>: iOS Safari ignores the `list` attribute entirely, so on
 * iPhone the home-page city field offered no suggestions at all while the form's
 * submit handler still demanded an exact match — the search simply refused to
 * run. See docs/wiki/pages/post-mortems/2026-08-mobile-city-search-datalist.md.
 */
(function (global) {
  'use strict';

  /* ── text normalisation ─────────────────────────────────────────────── */

  var MpText = {
    norm: function (s) {
      if (s == null) return '';
      var out = String(s).toLowerCase();
      try {
        out = out.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
      } catch (_) { /* ancient browser: fall through with the raw lowercase */ }
      // Hungarian double acutes decompose fine above; these cover the rest.
      return out.replace(/ß/g, 'ss').replace(/[đð]/g, 'd').replace(/ø/g, 'o')
                .replace(/æ/g, 'ae').replace(/ł/g, 'l')
                .replace(/[^a-z0-9]+/g, ' ').trim();
    },
    // First letter used for A-Z grouping. Accented letters group under their
    // base letter (Ó → O); anything non-alphabetic lands in the "#" bucket.
    initial: function (s) {
      var n = MpText.norm(s);
      var c = n.charAt(0).toUpperCase();
      return c >= 'A' && c <= 'Z' ? c : '#';
    },
  };

  /* ── autocomplete ───────────────────────────────────────────────────── */

  /* opts: { input, items, onPick, limit }
   * items: [{name, slug, count}] or ["Name", …]
   * Returns a controller with .resolve(value) → matched item or null. */
  function attachAutocomplete(opts) {
    var input = opts.input;
    if (!input) return null;

    var items = (opts.items || []).map(function (it) {
      var o = typeof it === 'string' ? { name: it } : it;
      return { name: o.name, slug: o.slug, count: o.count || 0, _n: MpText.norm(o.name) };
    });
    var limit = opts.limit || 8;

    // Mobile keyboards otherwise autocapitalise and autocorrect the field into
    // something that no longer matches any city name.
    input.setAttribute('autocomplete', 'off');
    input.setAttribute('autocapitalize', 'off');
    input.setAttribute('autocorrect', 'off');
    input.setAttribute('spellcheck', 'false');
    input.setAttribute('inputmode', 'search');
    input.setAttribute('enterkeyhint', 'search');
    input.setAttribute('role', 'combobox');
    input.setAttribute('aria-expanded', 'false');
    input.setAttribute('aria-autocomplete', 'list');
    input.removeAttribute('list');

    var wrap = input.parentNode;
    if (getComputedStyle(wrap).position === 'static') wrap.style.position = 'relative';

    var panel = document.createElement('ul');
    panel.className = 'mp-ac-panel';
    panel.setAttribute('role', 'listbox');
    panel.hidden = true;
    wrap.appendChild(panel);

    var open = false, active = -1, current = [];

    function match(q) {
      var n = MpText.norm(q);
      if (!n) return [];
      var starts = [], contains = [];
      for (var i = 0; i < items.length; i++) {
        var idx = items[i]._n.indexOf(n);
        if (idx === 0) starts.push(items[i]);
        else if (idx > 0) contains.push(items[i]);
        if (starts.length >= limit) break;
      }
      return starts.concat(contains).slice(0, limit);
    }

    function close() {
      open = false; active = -1; panel.hidden = true;
      input.setAttribute('aria-expanded', 'false');
    }

    function render(list) {
      current = list;
      if (!list.length) { close(); return; }
      panel.innerHTML = '';
      list.forEach(function (it, i) {
        var li = document.createElement('li');
        li.className = 'mp-ac-item';
        li.setAttribute('role', 'option');
        li.id = 'mp-ac-opt-' + i;
        var name = document.createElement('span');
        name.className = 'mp-ac-name';
        name.textContent = it.name;
        li.appendChild(name);
        if (it.count > 0) {
          var badge = document.createElement('span');
          badge.className = 'mp-ac-count';
          badge.textContent = it.count;
          li.appendChild(badge);
        }
        // pointerdown fires before the input's blur, so the pick survives.
        li.addEventListener('pointerdown', function (e) {
          e.preventDefault();
          pick(it);
        });
        panel.appendChild(li);
      });
      panel.hidden = false;
      open = true;
      input.setAttribute('aria-expanded', 'true');
    }

    function highlight(i) {
      var kids = panel.children;
      for (var k = 0; k < kids.length; k++) kids[k].classList.toggle('is-active', k === i);
      active = i;
      if (i >= 0) {
        input.setAttribute('aria-activedescendant', 'mp-ac-opt-' + i);
        if (kids[i].scrollIntoView) kids[i].scrollIntoView({ block: 'nearest' });
      } else {
        input.removeAttribute('aria-activedescendant');
      }
    }

    function pick(it) {
      input.value = it.name;
      close();
      if (opts.onPick) opts.onPick(it);
      input.dispatchEvent(new Event('mp:pick', { bubbles: true }));
    }

    input.addEventListener('input', function () { render(match(input.value)); });
    input.addEventListener('focus', function () {
      if (input.value.trim()) render(match(input.value));
    });
    input.addEventListener('blur', function () { setTimeout(close, 120); });

    input.addEventListener('keydown', function (e) {
      if (!open) {
        if (e.key === 'ArrowDown' && input.value.trim()) render(match(input.value));
        return;
      }
      if (e.key === 'ArrowDown') { e.preventDefault(); highlight(Math.min(active + 1, current.length - 1)); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); highlight(Math.max(active - 1, -1)); }
      else if (e.key === 'Enter') {
        if (active >= 0) { e.preventDefault(); pick(current[active]); }
        else close();
      } else if (e.key === 'Escape') { close(); }
    });

    return {
      /* Exact normalised match first, then a unique prefix match. Returns null
       * only when the text genuinely matches nothing — a lone near-miss is
       * accepted rather than blocking the submit. */
      resolve: function (value) {
        var n = MpText.norm(value);
        if (!n) return null;
        var exact = items.filter(function (it) { return it._n === n; });
        if (exact.length) return exact[0];
        var pref = items.filter(function (it) { return it._n.indexOf(n) === 0; });
        if (pref.length === 1) return pref[0];
        var sub = items.filter(function (it) { return it._n.indexOf(n) >= 0; });
        return sub.length === 1 ? sub[0] : null;
      },
      close: close,
    };
  }

  /* ── A-Z + free-text list filter ────────────────────────────────────── */

  /* opts: {
   *   input, list, itemSelector, nameAttr, azBar, countEl, emptyEl,
   *   groupSelector   — optional wrapper whose visibility follows its children
   * } */
  function attachListFilter(opts) {
    var input = opts.input;
    var list = opts.list;
    if (!list) return null;

    var items = [], groups = [], letters = {}, activeLetter = '';

    function collect() {
      items = Array.prototype.slice.call(
        list.querySelectorAll(opts.itemSelector || '[data-name]')
      ).map(function (el) {
        var name = el.getAttribute(opts.nameAttr || 'data-name') || el.textContent;
        return { el: el, name: name, n: MpText.norm(name), initial: MpText.initial(name) };
      });
      groups = opts.groupSelector
        ? Array.prototype.slice.call(list.querySelectorAll(opts.groupSelector))
        : [];
      letters = {};
      items.forEach(function (it) { letters[it.initial] = true; });
    }
    collect();

    if (input) {
      input.setAttribute('autocomplete', 'off');
      input.setAttribute('autocapitalize', 'off');
      input.setAttribute('autocorrect', 'off');
      input.setAttribute('spellcheck', 'false');
      input.setAttribute('inputmode', 'search');
    }

    function apply() {
      var q = MpText.norm(input ? input.value : '');
      var shown = 0;
      items.forEach(function (it) {
        var ok = (!q || it.n.indexOf(q) >= 0) &&
                 (!activeLetter || it.initial === activeLetter);
        it.el.hidden = !ok;
        // The explore page also has a topic-chip filter that hides cards with
        // an inline style. The two compose as an AND (the [hidden] rule is
        // !important), so the count must respect the other filter too or it
        // reports rows nobody can see.
        if (ok && it.el.style.display !== 'none') shown++;
      });
      groups.forEach(function (g) {
        var any = g.querySelector(
          (opts.itemSelector || '[data-name]') + ':not([hidden])'
        );
        g.hidden = !any;
      });
      if (opts.countEl) opts.countEl.textContent = shown;
      if (opts.emptyEl) opts.emptyEl.hidden = shown > 0;
      return shown;
    }

    function setLetter(ch) {
      activeLetter = activeLetter === ch ? '' : ch;
      if (opts.azBar) {
        Array.prototype.forEach.call(opts.azBar.children, function (b) {
          b.classList.toggle('is-active',
            (b.getAttribute('data-letter') || '') === activeLetter);
        });
      }
      apply();
    }

    function buildAzBar() {
      if (!opts.azBar) return;
      var bar = opts.azBar;
      bar.innerHTML = '';
      var all = document.createElement('button');
      all.type = 'button';
      all.className = 'mp-az-btn' + (activeLetter ? '' : ' is-active');
      all.textContent = bar.getAttribute('data-all-label') || 'A-Z';
      all.addEventListener('click', function () { setLetter(''); });
      bar.appendChild(all);

      '#ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('').forEach(function (ch) {
        if (!letters[ch]) return;
        var b = document.createElement('button');
        b.type = 'button';
        b.className = 'mp-az-btn' + (activeLetter === ch ? ' is-active' : '');
        b.textContent = ch;
        b.setAttribute('data-letter', ch);
        b.addEventListener('click', function () { setLetter(ch); });
        bar.appendChild(b);
      });
    }
    buildAzBar();

    if (input) {
      input.addEventListener('input', apply);
      // Enter on a single remaining result follows its link — the fastest path
      // on a phone, where tapping a small card is the slow part.
      input.addEventListener('keydown', function (e) {
        if (e.key !== 'Enter') return;
        e.preventDefault();
        var visible = items.filter(function (it) { return !it.el.hidden; });
        if (visible.length === 1) {
          var a = visible[0].el.tagName === 'A' ? visible[0].el : visible[0].el.querySelector('a');
          if (a && a.href) window.location.href = a.href;
        }
      });
    }

    apply();
    return {
      apply: apply,
      /* Re-read the DOM after items were added or removed. Pages that build
       * part of their list client-side (public_cities.html ships empty cities
       * as JSON) must call this, or the filter keeps a stale item list and the
       * A-Z bar is missing whole letters. */
      rescan: function () { collect(); buildAzBar(); return apply(); },
      get total() { return items.length; },
    };
  }

  /* ── auto-wiring ────────────────────────────────────────────────────── */

  /* Every [data-mp-filter] block rendered by templates/_listing_filter.html
   * wires itself up, so listing pages need no page-level JS at all. */
  var _attached = [];

  function autoAttach() {
    var bars = document.querySelectorAll('[data-mp-filter]');
    for (var i = 0; i < bars.length; i++) {
      var bar = bars[i];
      if (bar.dataset.mpAttached) continue;
      var list = document.querySelector(bar.getAttribute('data-target') || '');
      if (!list) continue;
      bar.dataset.mpAttached = '1';
      _attached.push(attachListFilter({
        input: bar.querySelector('[data-mp-filter-input]'),
        list: list,
        itemSelector: bar.getAttribute('data-item') || '[data-name]',
        groupSelector: bar.getAttribute('data-group') || '',
        azBar: bar.querySelector('[data-mp-filter-az]'),
        countEl: bar.querySelector('[data-mp-filter-count]'),
        emptyEl: bar.querySelector('[data-mp-filter-empty]'),
      }));
    }
  }

  /* Re-read every auto-attached filter. Called by pages that append items after
   * the initial attach. */
  function rescanAll() {
    autoAttach();  // pick up any filter block added since
    _attached.forEach(function (c) { if (c && c.rescan) c.rescan(); });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', autoAttach);
  } else {
    autoAttach();
  }

  global.MpText = MpText;
  global.MpAutocomplete = { attach: attachAutocomplete };
  global.MpListFilter = { attach: attachListFilter, autoAttach: autoAttach,
                          rescan: rescanAll };
})(window);
