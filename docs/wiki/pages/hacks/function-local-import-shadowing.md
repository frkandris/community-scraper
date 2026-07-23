---
type: Hack
title: Function-Local Import Shadows the Module-Level Import
description: A from-import inside one branch makes the name local to the entire function, so other branches crash with UnboundLocalError even though the module-level import exists.
tags: [python, imports, pitfall, web-app]
timestamp: 2026-07-23
resource: scraper/web/app.py
---

# Function-Local Import Shadows the Module-Level Import

*Python decides local-vs-global per function at compile time: any `from x import name` anywhere in a function makes `name` local everywhere in that function.*

Seen in production 2026-07-22: the explore route in `scraper/web/app.py` had `from ..db import get_city_topic_counts` inside the `if city:` branch while the country-grouped branch (no city selected) used the same name. With no city, the local import never executed and the branch raised `UnboundLocalError: cannot access local variable 'get_city_topic_counts'` — even though the module top already imported the function.

Rules for `app.py`, which uses many deliberate function-local imports:

- A function-local import is fine **only** if every use of that name in the function is after it, on every path.
- If the name is also imported at module level, delete the local import (the 2026-07-23 fix) or alias it (`import … as _gctc`, as the recently-added route does).
- The crash is invisible until the *other* branch runs — tests that always pass a city never catch it.

See [[web-app]].
