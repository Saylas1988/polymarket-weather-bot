"""
Автоверификация итога рынка после даты события: фактическая дневная tmax и победивший бакет.

Источник температуры: цепочка Polymarket resolving (см. polymarket_resolution_fetch) —
Weather Company для WU-городов, METAR/Ogimet для NOAA и как fallback для старых дат.
Open-Meteo Archive — только опциональная справка (MARKET_VERIFY_OPENMETEO_DEBUG).

Торговая логика не меняется; модуль только читает Gamma ladder и пишет журналы.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from typing import Any
from zoneinfo import ZoneInfo

import main as m
from openmeteo_config import archive_base_url, merge_openmeteo_auth, openmeteo_mode_label
from paper_manual_journal import append_market_verification_block
from paper_portfolio import load_portfolio, save_portfolio
from paper_settings import (
    market_verification_enabled,
    market_verify_min_full_days_after_event,
    market_verify_openmeteo_debug,
    paper_trade_journal_path,
    paper_verification_state_path,
)
from paper_trade_log import append_paper_trade_record
from polymarket_resolution_fetch import fetch_polymarket_resolution_temperature
from station_config import city_config_by_display_name

log = logging.getLogger("rainmaker")

_logged_verify_mode = False


def _log_market_verify_mode_once() -> None:
    global _logged_verify_mode
    if _logged_verify_mode:
        return
    _logged_verify_mode = True
    log.info(
        "Market verify: enabled=%s, state=%s, openmeteo_debug=%s, WEATHERCOM_API_KEY=%s",
        market_verification_enabled(),
        paper_verification_state_path(),
        market_verify_openmeteo_debug(),
        "set" if os.environ.get("WEATHERCOM_API_KEY", "").strip() else "default_embedded",
    )


def _default_verification_state() -> dict[str, Any]:
    return {"version": 1, "by_slug": {}}


def load_verification_state() -> dict[str, Any]:
    path = paper_verification_state_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return _default_verification_state()
        data.setdefault("by_slug", {})
        return data
    except FileNotFoundError:
        return _default_verification_state()
    except Exception as e:
        log.warning("market verify: не удалось прочитать state %s: %s", path, e)
        return _default_verification_state()


def save_verification_state(data: dict[str, Any]) -> None:
    path = paper_verification_state_path()
    tmp = path + ".tmp"
    payload = dict(data)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _collect_event_slugs_from_portfolio(portfolio: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for slug in (portfolio.get("open_positions") or {}).keys():
        if isinstance(slug, str) and slug.strip():
            out.add(slug.strip())
    for row in portfolio.get("closed_positions") or []:
        if not isinstance(row, dict):
            continue
        s = row.get("event_slug")
        if isinstance(s, str) and s.strip():
            out.add(s.strip())
    return out


def build_temperature_ladder_from_gamma_event(event: dict) -> tuple[list[dict], str] | None:
    """Та же геометрия бакетов, что в evaluate_signal_for_event (без ensemble)."""
    markets = event.get("markets")
    if not isinstance(markets, list):
        return None
    ladder: list[dict] = []
    ladder_unit: str | None = None
    for mk in markets:
        if not isinstance(mk, dict):
            continue
        question = mk.get("question") or mk.get("title") or ""
        if not isinstance(question, str) or not question.strip():
            continue
        outcomes = m._extract_outcomes_field(mk)
        prices = m._extract_outcome_prices_field(mk)
        if len(outcomes) != 2 or len(prices) != 2:
            continue
        low, high, unit = m._extract_range_from_question(question)
        if ladder_unit is None:
            ladder_unit = unit
        if ladder_unit != unit:
            continue
        liq = m._yes_price_and_liquidity(mk)
        gm_id, cond_id = m._gamma_market_stable_ids(mk)
        ladder.append(
            {
                "question": question.strip(),
                "low": low,
                "high": high,
                "gamma_market_id": gm_id,
                "condition_id": cond_id,
                **liq,
            }
        )
    if not ladder or ladder_unit is None:
        return None
    return ladder, ladder_unit


def pick_winning_bucket(
    observed_in_ladder_unit: float, ladder: list[dict], ladder_unit: str
) -> dict[str, Any]:
    """Тот же критерий попадания, что для control member в evaluate_signal_for_event."""

    def key(x: dict) -> int:
        return -10_000 if x["low"] is None else int(x["low"])

    ladder = list(ladder)
    ladder.sort(key=key)
    forecast = float(observed_in_ladder_unit)

    def contains(x: dict) -> bool:
        low = x["low"]
        high = x["high"]
        if low is None and high is not None:
            return forecast <= high
        if high is None and low is not None:
            return forecast >= low
        if low is not None and high is not None:
            return low <= forecast <= high
        return False

    main_idx = None
    for i, x in enumerate(ladder):
        if contains(x):
            main_idx = i
            break
    if main_idx is None:

        def dist(x: dict) -> float:
            low, high = x["low"], x["high"]
            if low is None and high is not None:
                center = float(high)
            elif high is None and low is not None:
                center = float(low)
            else:
                center = (float(low) + float(high)) / 2.0
            return abs(center - forecast)

        main_idx = min(range(len(ladder)), key=lambda i: dist(ladder[i]))

    row = ladder[main_idx]
    label = str(row.get("question") or "?")
    return {
        "winning_bucket_label": label,
        "winning_bucket": row,
        "winning_index": main_idx,
        "ladder_unit": ladder_unit,
    }


def fetch_openmeteo_archive_reference_c(
    *,
    lat: float,
    lon: float,
    event_day: dt.date,
    tz_name: str,
) -> float | None:
    """Только справка / отладка — не основной итог рынка."""
    url = archive_base_url()
    params = merge_openmeteo_auth(
        {
            "latitude": lat,
            "longitude": lon,
            "start_date": event_day.isoformat(),
            "end_date": event_day.isoformat(),
            "daily": "temperature_2m_max",
            "timezone": tz_name,
        }
    )
    try:
        r = m.HTTP.get(url, params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()
    except Exception as e:
        log.debug("market verify: openmeteo reference HTTP error %s: %s", url, e)
        return None
    daily = payload.get("daily") if isinstance(payload, dict) else None
    if not isinstance(daily, dict):
        return None
    maxs = daily.get("temperature_2m_max")
    if not isinstance(maxs, list) or len(maxs) == 0:
        return None
    v = maxs[0]
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _paper_journal_has_market_verified_line(journal_path: str, event_slug: str) -> bool:
    needle_action = '"action": "market_verified"'
    needle_slug = f'"event_slug": "{event_slug}"'
    try:
        with open(journal_path, "r", encoding="utf-8") as f:
            chunk = f.read()
        if needle_slug not in chunk:
            return False
        # грубая проверка: оба фрагмента в одной строке JSONL
        for line in chunk.splitlines()[-400:]:
            if needle_action in line and needle_slug in line:
                return True
    except FileNotFoundError:
        return False
    except OSError:
        return False
    return False


def _attach_verification_to_portfolio(portfolio: dict[str, Any], event_slug: str, ver: dict[str, Any]) -> None:
    op = portfolio.setdefault("open_positions", {})
    if event_slug in op and isinstance(op[event_slug], dict):
        op[event_slug]["market_verification"] = dict(ver)
    for c in portfolio.get("closed_positions") or []:
        if isinstance(c, dict) and c.get("event_slug") == event_slug:
            c["market_verification"] = dict(ver)


def verify_one_event_slug(event_slug: str, *, now: dt.datetime | None = None) -> dict[str, Any] | None:
    """
    Полная верификация одного slug (без проверки state — вызывающий решает).
    Возвращает dict для записи в state или None при отложенной попытке / ошибке.
    """
    now = now or dt.datetime.now(dt.timezone.utc)
    if not event_slug or not event_slug.strip():
        return None
    event_slug = event_slug.strip()

    city_name = m._parse_city_from_event_slug(event_slug)
    event_date = m._parse_event_slug_date(event_slug)
    tz_name = m.CITY_TIMEZONE.get(city_name)
    if not tz_name:
        log.warning("market verify: нет таймзоны для %s", city_name)
        return None

    local_today = dt.datetime.now(ZoneInfo(tz_name)).date()
    min_gap_days = 1 + market_verify_min_full_days_after_event()
    if (local_today - event_date).days < min_gap_days:
        return None

    cfg = city_config_by_display_name(city_name)
    lat, lon = float(cfg.station_lat), float(cfg.station_lon)

    res = fetch_polymarket_resolution_temperature(
        cfg, event_date=event_date, tz_name=tz_name, local_today=local_today
    )
    tmax_c = res.verified_temperature_c
    if tmax_c is None:
        log.debug(
            "market verify: нет tmax из resolving chain для %s %s (%s) — позже",
            event_slug,
            event_date.isoformat(),
            res.verification_method,
        )
        return None

    try:
        event = m._gamma_get_event_by_slug(event_slug)
    except Exception as e:
        log.warning("market verify: Gamma %s: %s", event_slug, e)
        return None

    built = build_temperature_ladder_from_gamma_event(event)
    if built is None:
        log.warning("market verify: нет лестницы %s", event_slug)
        return None
    ladder, ladder_unit = built

    if ladder_unit == "C":
        observed_market = tmax_c
    else:
        observed_market = m._c_to_f(tmax_c)

    win = pick_winning_bucket(observed_market, ladder, ladder_unit)

    verified_at = now.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    out: dict[str, Any] = {
        "event_slug": event_slug,
        "city": city_name,
        "event_date": event_date.isoformat(),
        "verified_at_utc": verified_at,
        "verified_temperature_c": round(tmax_c, 4),
        "observed_in_ladder_unit": round(float(observed_market), 4),
        "ladder_unit": ladder_unit,
        "winning_bucket_label": win["winning_bucket_label"],
        "source_type": cfg.source_type,
        "source_of_truth": res.source_of_truth,
        "resolution_url": cfg.resolution_url,
        "station_code": cfg.station_code,
        "forecast_lat": cfg.station_lat,
        "forecast_lon": cfg.station_lon,
        "timezone": tz_name,
        "verification_method": res.verification_method,
        "verification_detail": res.detail,
        "verification_note": res.verification_note,
    }
    if market_verify_openmeteo_debug():
        om = fetch_openmeteo_archive_reference_c(lat=lat, lon=lon, event_day=event_date, tz_name=tz_name)
        out["openmeteo_reference_temperature_c"] = om
        out["openmeteo_mode"] = openmeteo_mode_label()
    return out


def run_market_outcome_verification_pass(*, now: dt.datetime | None = None) -> int:
    """
    Проход по event_slug из paper portfolio; успешно верифицированные помечаются в state (дедуп).
    Возвращает число новых верификаций за вызов.
    """
    if not market_verification_enabled():
        return 0

    _log_market_verify_mode_once()
    now = now or dt.datetime.now(dt.timezone.utc)

    portfolio = load_portfolio()
    slugs = _collect_event_slugs_from_portfolio(portfolio)
    if not slugs:
        return 0

    state = load_verification_state()
    by_slug: dict[str, Any] = state.setdefault("by_slug", {})
    journal_path = paper_trade_journal_path()

    done = 0
    for event_slug in sorted(slugs):
        if event_slug in by_slug:
            continue
        try:
            ver = verify_one_event_slug(event_slug, now=now)
        except Exception:
            log.exception("market verify: исключение для %s", event_slug)
            continue
        if ver is None:
            continue

        manual_ok = append_market_verification_block(
            event_slug=event_slug,
            winning_bucket_label=str(ver["winning_bucket_label"]),
            verified_temperature_c=float(ver["verified_temperature_c"]),
            verification_method=str(ver["verification_method"]),
            resolution_hint=str(ver["resolution_url"]),
            ladder_unit=str(ver["ladder_unit"]),
            observed_in_market_unit=float(ver["observed_in_ladder_unit"]),
            source_of_truth=str(ver.get("source_of_truth") or ""),
        )
        if not manual_ok:
            log.warning("market verify: автожурнал не записан для %s — state не сохраняем, повтор позже", event_slug)
            continue

        if not _paper_journal_has_market_verified_line(journal_path, event_slug):
            j_ok = append_paper_trade_record(
                {
                    "action": "market_verified",
                    "at_utc": ver["verified_at_utc"],
                    **ver,
                },
                path=journal_path,
            )
            if not j_ok:
                log.warning(
                    "market verify: paper JSONL не записан для %s — state не сохраняем, повтор позже",
                    event_slug,
                )
                continue

        _attach_verification_to_portfolio(portfolio, event_slug, ver)
        save_portfolio(portfolio)

        by_slug[event_slug] = ver
        save_verification_state(state)
        done += 1
        log.info(
            "market verify: %s → %s, %.2f°C",
            event_slug[:56],
            ver["winning_bucket_label"][:48],
            ver["verified_temperature_c"],
        )

    return done
