---
type: Hack
title: Jinja2 Macros Must Be Defined Before Use
description: Jinja2 does not hoist macro definitions; a macro called before its block fails at render, silently if the branch is skipped.
tags: [jinja2, templates, macros]
timestamp: 2026-07-09
resource: scraper/web/templates
---

# Jinja2 Macros Must Be Defined Before Use

*Jinja2 does NOT hoist macro definitions. A macro called before its `{% macro %}` block is defined causes `UndefinedError` at render time — and silently succeeds if the calling branch is never reached.*

## The gotcha

```jinja2
{# WRONG — macro is called before it's defined #}
{% if records %}
  {{ render_row(records[0]) }}
{% endif %}

{% macro render_row(r) %}
  ...
{% endmacro %}
```

This will work fine until `records` is non-empty, at which point it raises `UndefinedError`.

## The fix

Always define macros at the top of the template, before any usage:

```jinja2
{% macro render_row(r) %}
  ...
{% endmacro %}

{% if records %}
  {{ render_row(records[0]) }}
{% endif %}
```

## Why it's hard to catch

The bug only surfaces when the calling branch is actually reached. A template that works in development (where `records` is empty) breaks silently in production. There's no Jinja2 lint that catches this.

## Related

- [[jinja2-namespace-mutable-counter]]
