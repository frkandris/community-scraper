# Jinja2: Namespace for Mutable Variables Inside Loops

*Jinja2 scoping rules prevent modifying outer variables from inside a `{% for %}` block. Use `{% set ns = namespace(n=0) %}` and `ns.n` instead.*

## The problem

```jinja2
{% set total = 0 %}
{% for item in items %}
  {% set total = total + 1 %}  {# This does nothing — sets a loop-scoped variable #}
{% endfor %}
{{ total }}  {# Always 0 #}
```

## The fix

```jinja2
{% set ns = namespace(n=0) %}
{% for item in items %}
  {% set ns.n = ns.n + 1 %}
{% endfor %}
{{ ns.n }}  {# Correct #}
```

## Where this pattern is used in this project

`coverage.html` uses multiple `namespace` objects to count cells across nested `{% for country %}`, `{% for city %}`, `{% for topic %}` loops.

## Related

- [[jinja2-macro-definition-order]]
