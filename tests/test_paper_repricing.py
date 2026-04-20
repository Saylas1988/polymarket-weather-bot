"""Repricing v2: дедлайн time exit и правила model drift."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

from paper_engine import _repricing_drift_should_exit, _repricing_time_exit_deadline_utc


class _M:
    CITY_TIMEZONE = {"London": "Europe/London"}


def test_time_exit_deadline_london_midnight_minus_hours():
    res = {"date": "2026-07-10", "city": "London"}
    dl = _repricing_time_exit_deadline_utc(res, _M())
    assert dl is not None
    # local midnight Europe/London 2026-07-10 -> UTC depends on BST; deadline = that - 18h
    ev = dt.datetime(2026, 7, 10, 0, 0, tzinfo=ZoneInfo("Europe/London"))
    expected = (ev - dt.timedelta(hours=18)).astimezone(dt.timezone.utc).replace(microsecond=0).isoformat()
    assert dl == expected


def test_drift_p_main_drop_triggers():
    pos = {
        "repricing_meta": {
            "p_main_entry": 0.50,
            "trend_at_entry": "stronger",
            "main_gamma_market_id_at_entry": "gm1",
        }
    }
    cur = {"skip": False, "p_main": 0.40, "trend_label": "unchanged", "main": {"gamma_market_id": "gm1"}}
    ok, d = _repricing_drift_should_exit(pos, cur)
    assert ok is True
    assert "p_main_drop" in d.get("rules_triggered", [])


def test_drift_main_bucket_change():
    pos = {
        "repricing_meta": {
            "p_main_entry": 0.50,
            "trend_at_entry": "unchanged",
            "main_gamma_market_id_at_entry": "gm1",
        }
    }
    cur = {"skip": False, "p_main": 0.49, "trend_label": "unchanged", "main": {"gamma_market_id": "gm2"}}
    ok, d = _repricing_drift_should_exit(pos, cur)
    assert ok is True
    assert "main_bucket_change" in d.get("rules_triggered", [])


def test_drift_trend_to_weaker():
    pos = {
        "repricing_meta": {
            "p_main_entry": 0.50,
            "trend_at_entry": "stronger",
            "main_gamma_market_id_at_entry": "gm1",
        }
    }
    cur = {"skip": False, "p_main": 0.50, "trend_label": "weaker", "main": {"gamma_market_id": "gm1"}}
    ok, d = _repricing_drift_should_exit(pos, cur)
    assert ok is True
    assert "trend_to_weaker" in d.get("rules_triggered", [])


def test_skip_eval_no_drift():
    pos = {"repricing_meta": {"p_main_entry": 0.5, "main_gamma_market_id_at_entry": "x"}}
    cur = {"skip": True, "reason": "openmeteo_ensemble_unavailable"}
    ok, d = _repricing_drift_should_exit(pos, cur)
    assert ok is False
