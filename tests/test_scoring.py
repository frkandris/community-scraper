

def test_the_golden_set_can_be_restricted_to_one_market(tmp_path):
    """A ranking measured on the wrong workload describes the wrong thing.

    The corpus is roughly 30% Hungarian and 70% international, so an unfiltered
    sample is mostly English pages — while Hungarian is the primary market. A
    sibling project found this the expensive way: the model that scored better
    on its English sample dropped into English mid-answer on the real Hungarian
    task and lost half the required fields.
    """
    import json

    from scraper.db import init_db
    from scraper.scoring import golden_set

    db = tmp_path / "s.db"
    init_db(db)

    def _page(url, locale, name):
        import hashlib
        import sqlite3
        blob = json.dumps({
            "raw_text": "elég hosszú oldalszöveg " * 20,
            "records": [{"name": name, "locale": locale}],
        })
        with sqlite3.connect(db) as conn:
            conn.execute(
                "INSERT INTO cache_pages (url_hash, url, city, topic, domain,"
                " scraped_at, extracted_at, data) VALUES (?,?,?,?,?,?,?,?)",
                (hashlib.sha256(url.encode()).hexdigest()[:16], url, "X", "running",
                 "t", "2026-01-01", "2026-01-01", blob))

    _page("https://a.test", "hu", "Szentendrei Futóklub")
    _page("https://b.test", "en", "Brighton Runners")
    _page("https://c.test", "hu", "Pécsi Sakk Kör")

    assert len(golden_set(db, limit=10)) == 3               # every market
    hu = golden_set(db, limit=10, locale="hu")
    # A set: the order comes from url_hash, which is how the sample is kept
    # stable, not something a caller should depend on.
    assert {p["url"] for p in hu} == {"https://a.test", "https://c.test"}
    assert len(golden_set(db, limit=10, locale="en")) == 1
    assert golden_set(db, limit=10, locale="sv") == []
