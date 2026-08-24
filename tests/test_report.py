

def test_the_report_states_what_was_spent_and_what_came_out():
    """"How much did we spend, and what did it produce?"

    It used to derive a "~N pages/day capacity" from that. Five versions of
    that number were wrong in one morning, and the last review closed it: the
    budget is not one scalar — Groq's binding limit is 200,000 tokens a day,
    not its 14,400 requests — so summing request allowances and dividing by
    attempts-per-page has no answer. Measured quantities only; pages per day is
    an observation across reports, not a derivation inside one.
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
        "enrich_attempts": 120, "extract_attempts": 400,
        "providers": [
            {"name": "groq", "used": 400, "budget": 1000, "failures": 100},
            {"name": "gemini", "used": 200, "budget": 1000, "failures": 100},
        ],
    }
    _subject, html = build_report_html("2026-08-18", summary, {})
    text = " ".join(_html.unescape(re.sub(r"<[^>]+>", " ", html)).split())

    assert "600 hívás" in text
    assert "400 kinyerés" in text
    assert "120 leírás" in text
    # 600 - 400 - 120: preflight and the gateway, named rather than folded in.
    assert "80 egyéb" in text
    assert "353 feldolgozott oldal" in text
    assert "6.9×" in text          # fetched vs processed
    assert "33%" in text           # refused calls
    # The number that cannot be computed from this accounting.
    assert "kapacitás" not in text
    assert "hívás/oldal" not in text


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
