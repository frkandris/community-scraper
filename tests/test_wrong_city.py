from pathlib import Path

from scraper.db import (
    init_db,
    get_wrong_city_candidates,
    resolve_wrong_city_candidate,
    set_community_hidden,
    _community_record_key,
)
from scraper.models import CommunityRecord
from scraper.store import save_results
from scraper.wrong_city import (
    cleanup_stale_wrong_city_candidates,
    detect_wrong_city_candidates,
    scan,
)

CITIES = ["Budapest", "Debrecen", "Szentendre", "Szentgotthárd", "Vác", "Vácrátót"]


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def _save(db, name, city, topic="seniors", **fields):
    r = CommunityRecord(
        name=name, topic=topic, city=city, locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
        **fields,
    )
    save_results(city, topic, [r], db)


def test_detects_other_city_in_description(tmp_path):
    db = _db(tmp_path)
    _save(db, "Nappali Idősek Klubja", "Szentendre",
          description="A szentgotthárdi nyugdíjasok heti klubja Szentgotthárdon.")
    assert detect_wrong_city_candidates(db, CITIES) == 1
    cands = get_wrong_city_candidates(db)
    assert len(cands) == 1
    c = cands[0]
    assert c["mentioned_city"] == "Szentgotthárd"
    assert c["field"] == "description"
    assert "szentgotthárdi" in c["matched_text"].lower() or "szentgotthárdon" in c["matched_text"].lower()
    assert c["record_key"] == _community_record_key("Nappali Idősek Klubja", "Szentendre", "seniors")


def test_own_city_mention_not_flagged(tmp_path):
    db = _db(tmp_path)
    _save(db, "Szentendrei Kör", "Szentendre",
          description="A szentendrei tagok Szentendrén találkoznak.")
    assert detect_wrong_city_candidates(db, CITIES) == 0


def test_longer_city_name_wins_over_prefix(tmp_path):
    db = _db(tmp_path)
    # 'Vácrátót' is a known city — must not be reported as a 'Vác' mention,
    # and a Vácrátót community mentioning only itself is clean.
    _save(db, "Kertbarátok", "Vácrátót", description="A vácrátóti botanikus kertnél.")
    assert detect_wrong_city_candidates(db, CITIES) == 0
    _save(db, "Sakk Klub", "Budapest", description="Korábban Vácrátóton működött.")
    assert detect_wrong_city_candidates(db, CITIES) == 1
    assert get_wrong_city_candidates(db)[0]["mentioned_city"] == "Vácrátót"


def test_rescan_is_idempotent_and_dismissed_stays_dismissed(tmp_path):
    db = _db(tmp_path)
    _save(db, "Klub", "Szentendre", description="Szentgotthárdon alakult.")
    assert detect_wrong_city_candidates(db, CITIES) == 1
    assert detect_wrong_city_candidates(db, CITIES) == 0
    cand = get_wrong_city_candidates(db)[0]
    resolve_wrong_city_candidate(db, cand["id"], "dismissed")
    assert detect_wrong_city_candidates(db, CITIES) == 0
    assert get_wrong_city_candidates(db) == []


def test_cleanup_dismisses_hidden_or_changed_records(tmp_path):
    db = _db(tmp_path)
    _save(db, "Klub", "Szentendre", description="Szentgotthárdon alakult.")
    assert scan(db, CITIES) == 1
    key = _community_record_key("Klub", "Szentendre", "seniors")
    set_community_hidden(db, key, True)
    # Hidden record still resolves by key, but a deleted/re-keyed one does not:
    # simulate by pointing the candidate at a record that no longer exists.
    assert cleanup_stale_wrong_city_candidates(db) == 0  # text still mentions the city
    _save(db, "Klub", "Szentendre", description="Helyi klub.")  # description no longer mentions it
    set_community_hidden(db, key, False)
    assert cleanup_stale_wrong_city_candidates(db) == 1
    assert get_wrong_city_candidates(db) == []
