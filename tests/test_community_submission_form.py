from pathlib import Path
from scraper.db import init_db, get_community_submissions
from scraper.web import app as web_app
from scraper.web.state import app_state
from scraper.pipeline import CityConfig, TopicConfig
from fastapi.testclient import TestClient


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def _setup_state(tmp_db):
    app_state.db_path = tmp_db
    app_state.cities = [
        CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[]),
    ]
    app_state.topics = [
        TopicConfig(name="running", search_terms={}),
        TopicConfig(name="yoga", search_terms={}),
    ]


def test_get_form_returns_200(tmp_path):
    db = _db(tmp_path)
    old_db, old_cities, old_topics = app_state.db_path, app_state.cities, app_state.topics
    try:
        _setup_state(db)
        resp = TestClient(web_app.app).get("/kozosseg-bekuldes")
        assert resp.status_code == 200
        assert "form" in resp.text.lower()
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
        app_state.topics = old_topics


def test_get_form_prefills_city_and_topic(tmp_path):
    db = _db(tmp_path)
    old_db, old_cities, old_topics = app_state.db_path, app_state.cities, app_state.topics
    try:
        _setup_state(db)
        resp = TestClient(web_app.app).get("/kozosseg-bekuldes?city=Budapest&topic=running")
        assert resp.status_code == 200
        assert "Budapest" in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
        app_state.topics = old_topics


def test_post_form_saves_submission_and_redirects(tmp_path):
    db = _db(tmp_path)
    old_db, old_cities, old_topics = app_state.db_path, app_state.cities, app_state.topics
    try:
        _setup_state(db)
        resp = TestClient(web_app.app, follow_redirects=False).post(
            "/kozosseg-bekuldes",
            data={
                "name": "Budapest Futók",
                "city": "Budapest",
                "topic": "running",
                "source_url": "https://example.com/futok",
                "submitter_email": "",
            },
        )
        assert resp.status_code == 302
        assert "submitted=1" in resp.headers["location"]
        rows = get_community_submissions(db, status="pending")
        assert len(rows) == 1
        assert rows[0]["name"] == "Budapest Futók"
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
        app_state.topics = old_topics


def test_post_form_missing_required_field_returns_400(tmp_path):
    db = _db(tmp_path)
    old_db, old_cities, old_topics = app_state.db_path, app_state.cities, app_state.topics
    try:
        _setup_state(db)
        resp = TestClient(web_app.app).post(
            "/kozosseg-bekuldes",
            data={"name": "", "city": "Budapest", "topic": "running", "source_url": "https://x.com"},
        )
        assert resp.status_code == 400
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
        app_state.topics = old_topics


def test_post_form_rejects_non_public_source_url(tmp_path):
    db = _db(tmp_path)
    old_db, old_cities, old_topics = app_state.db_path, app_state.cities, app_state.topics
    try:
        _setup_state(db)
        resp = TestClient(web_app.app).post(
            "/kozosseg-bekuldes",
            data={
                "name": "Belső szolgáltatás",
                "city": "Budapest",
                "topic": "running",
                "source_url": "http://169.254.169.254/latest/meta-data",
            },
        )

        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_source_url"
        assert get_community_submissions(db, status="pending") == []
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
        app_state.topics = old_topics


def test_get_form_submitted_shows_thank_you(tmp_path):
    db = _db(tmp_path)
    old_db, old_cities, old_topics = app_state.db_path, app_state.cities, app_state.topics
    try:
        _setup_state(db)
        resp = TestClient(web_app.app).get("/kozosseg-bekuldes?submitted=1")
        assert resp.status_code == 200
        assert "köszön" in resp.text.lower()
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
        app_state.topics = old_topics
