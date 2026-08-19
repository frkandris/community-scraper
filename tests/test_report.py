

def test_the_report_states_the_daily_processing_capacity():
    """"How much can we process in a day, and are we collecting that much?"

    Both numbers were already in the email and the answer still took an
    afternoon of arithmetic: on 2026-08-18 we fetched 2,434 pages and
    AI-processed 353 of them.
    """
    import html as _html
    import re

    from scraper.report import build_report_html

    hu = dict(new_communities=78, changed_communities=500, change_rows=1199,
              new_venues=24, new_persons=79, searches=900,
              pages_scraped=2434, pages_extracted=301)
    intl = dict(new_communities=29, changed_communities=0, change_rows=0,
                new_venues=13, new_persons=11, searches=0,
                pages_scraped=0, pages_extracted=52)
    summary = {
        "hu": hu, "intl": intl,
        "totals": {"hu": 12536, "intl": 29493,
                   "covered_pairs_hu": 13911, "covered_pairs_intl": 32769},
        "runs": [],
        "providers": [
            {"name": "groq", "used": 400, "budget": 1000, "failures": 100},
            {"name": "gemini", "used": 200, "budget": 1000, "failures": 100},
        ],
    }
    _subject, html = build_report_html("2026-08-18", summary, {})
    # Tags become spaces, so collapse runs of whitespace before matching —
    # otherwise "</b> kapacitás" reads as two spaces.
    text = " ".join(_html.unescape(re.sub(r"<[^>]+>", " ", html)).split())

    # 600 calls, 200 failed -> 400 successful over 353 pages ≈ 1.1 calls/page,
    # and 2,000 of budget therefore buys about 1,700 pages.
    assert "1.1 hívás/oldal" in text
    assert "oldal/nap kapacitás" in text
    assert "6.9×" in text          # fetched vs processed
    assert "33%" in text           # refused calls


def test_the_capacity_line_is_omitted_when_nothing_was_processed():
    """No pages means no ratio — printing 'inf pages/day' would be worse."""
    import re

    from scraper.report import build_report_html

    zero = dict(new_communities=0, changed_communities=0, change_rows=0,
                new_venues=0, new_persons=0, searches=0,
                pages_scraped=0, pages_extracted=0)
    summary = {
        "hu": zero, "intl": dict(zero), "runs": [],
        "totals": {"hu": 0, "intl": 0, "covered_pairs_hu": 0, "covered_pairs_intl": 0},
        "providers": [{"name": "groq", "used": 0, "budget": 1000, "failures": 0}],
    }
    _subject, html = build_report_html("2026-08-18", summary, {})
    assert "hívás/oldal" not in re.sub(r"<[^>]+>", " ", html)
