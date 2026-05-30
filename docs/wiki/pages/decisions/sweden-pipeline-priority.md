# Decision: Sweden as Second Pipeline Priority (After Hungary)

*Added in May 2026 when 290 Swedish municipalities were added to the config.*

## Priority order

1. Hungary (primary market — Hungarian-language site közösségek.com)
2. Sweden (second largest city list at 290 municipalities)
3. Everything else

## Implementation

`main.py` splits `app_state.cities` into three lists and runs three sequential `run_pipeline()` calls:

```python
hu_cities = [c for c in cities if c.country == "Hungary"]
se_cities = [c for c in cities if c.country == "Sweden"]
intl_cities = [c for c in cities if c.country not in {"Hungary", "Sweden"}]
```

## Why not a single call

A single call processes cities in the order they appear in `cities.yaml`. Splitting gives explicit control over priority without reordering the YAML, and makes the coverage page's country-tab display reflect actual pipeline order.

## Why Sweden second

290 municipalities is the largest non-Hungarian city list. Running it before international cities ensures Swedish coverage progresses even if the pipeline is stopped mid-run.

## Related

- [[pipeline-run-modes]]
- [[cities-yaml-structure]]
