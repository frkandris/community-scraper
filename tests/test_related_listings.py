"""Related listings at the foot of a detail page.

23,461 of the site's pages sit in Google's "Crawled – currently not indexed":
fetched, looked at, and judged not worth keeping. A community page's unique
content was a name, a city, a topic and often a single line, inside a template
identical across tens of thousands of URLs. These sections are content and a
crawl path at once.
"""
from scraper.web.app import related_communities


def _rec(name, city="Szentendre", topic="running"):
    return {"name": name, "city": city, "topic": topic}


def test_siblings_are_split_by_topic():
    """"Other running clubs here" and "what else happens here" are two questions."""
    records = [
        _rec("Szentendrei Futóklub"),
        _rec("Duna-parti Futók"),
        _rec("Szentendrei Sakk Kör", topic="chess"),
        _rec("Városi Kórus", topic="choir"),
    ]
    out = related_communities(records, exclude_key="", topic="running", locale="hu")

    assert [i["name"] for i in out["same_topic"]] == [
        "Szentendrei Futóklub", "Duna-parti Futók"]
    assert [i["name"] for i in out["other_topics"]] == [
        "Szentendrei Sakk Kör", "Városi Kórus"]


def test_the_page_does_not_link_to_itself():
    from scraper.web.app import _community_record_key

    me = _rec("Szentendrei Futóklub")
    key = _community_record_key(me["name"], me["city"], me["topic"])
    out = related_communities([me, _rec("Duna-parti Futók")],
                              exclude_key=key, topic="running", locale="hu")
    assert [i["name"] for i in out["same_topic"]] == ["Duna-parti Futók"]


def test_links_point_at_the_community_url():
    out = related_communities([_rec("Szentendrei Futóklub")],
                              exclude_key="", topic="running", locale="hu")
    assert out["same_topic"][0]["url"] == "/szentendre/szentendrei-futoklub"
    # The note names the topic in the reader's language, not the internal slug.
    assert out["same_topic"][0]["note"] != "running"


def test_a_screenful_not_a_dump():
    """Budapest has hundreds, and the Tailwind CDN scans the DOM before paint."""
    records = [_rec(f"Klub {i}", city="Budapest") for i in range(200)]
    out = related_communities(records, exclude_key="", topic="running",
                              locale="hu", limit=12)

    assert len(out["same_topic"]) == 12
    # …but the total is kept, so the page can honestly say how many more.
    assert out["same_topic_total"] == 200


def test_records_without_a_name_or_city_are_skipped():
    """A link with no name is a link nobody can follow, and neither can a crawler."""
    out = related_communities(
        [{"name": "", "city": "Szentendre", "topic": "running"},
         {"name": "Névtelen", "city": "", "topic": "running"},
         _rec("Valódi Klub")],
        exclude_key="", topic="running", locale="hu")
    assert [i["name"] for i in out["same_topic"]] == ["Valódi Klub"]
