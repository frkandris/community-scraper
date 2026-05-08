import pytest

from scraper.main import DEFAULT_SCHEDULE_CRON, _cron_fields


def test_cron_fields_accepts_standard_five_field_cron():
    assert _cron_fields("0 3 * * *") == ("0", "3", "*", "*", "*")


def test_cron_fields_falls_back_for_invalid_cron():
    assert _cron_fields("not enough fields") == tuple(DEFAULT_SCHEDULE_CRON.split())


def test_cron_fields_rejects_invalid_fallback():
    with pytest.raises(ValueError):
        _cron_fields("bad", fallback="also bad")
