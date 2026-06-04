# init_db() Runs Before Prompt Overrides Load

`init_db()` is called at startup, before the admin prompt overrides are loaded from the database. This means `get_extract_fingerprint()` returns a different value inside `init_db()` than it does during normal request handling.

## Consequence

Any migration logic placed in `init_db()` that references the "current" fingerprint (e.g. `UPDATE cache_pages SET extract_fingerprint = ?`) will use the **wrong** fingerprint if the user has stored prompt overrides. Static hardcoded hashes are equally wrong — they'll never match the runtime fingerprint.

## Example failure

An `init_db()` migration added:
```python
conn.execute(
    "UPDATE cache_pages SET extract_fingerprint = ? WHERE extract_fingerprint = '88632fe6dadc'",
    (get_extract_fingerprint(),)
)
```
This found 0 rows in production because the actual stored fingerprint was computed after overrides were applied.

## Correct pattern

Put fingerprint migrations in a **runtime endpoint**, not `init_db()`:

```python
@admin.post("/api/restamp-fingerprints")
async def api_restamp_fingerprints():
    from ..db import _connect
    from ..extract import get_extract_fingerprint
    current_fp = get_extract_fingerprint()   # overrides already loaded
    with _connect(app_state.db_path) as conn:
        cur = conn.execute(
            "UPDATE cache_pages SET extract_fingerprint = ? WHERE extract_fingerprint != ?",
            (current_fp, current_fp),
        )
        conn.commit()
    return {"updated": cur.rowcount, "fingerprint": current_fp}
```

Trigger via curl (requires `Origin` header to pass the CSRF check):
```
curl -X POST https://kozossegek.com/admin/api/restamp-fingerprints \
  -u admin:$PASS -H "Origin: https://kozossegek.com"
```
