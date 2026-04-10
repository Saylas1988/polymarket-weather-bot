"""
In-memory cache + metrics for Open-Meteo ECMWF ensemble HTTP responses.

Снижает дублирующие запросы (один ответ покрывает D+1..D+3 для одного города)
и даёт stale fallback при 429 без Redis.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

import requests

log = logging.getLogger("rainmaker")

# Дефолт 2700 с (45 мин): между типичными циклами ~30 мин чаще попадаем в hit,
# но данные не «застывают» на сутки; в пределах запрошенного пользователем 1800–3600.
_DEFAULT_TTL = 2700
_DEFAULT_STALE_MAX = 14_400  # 4 ч — насколько старый кэш ещё допустим при 429


class OpenMeteoEnsembleUnavailable(Exception):
    """Нет данных ensemble (в т.ч. после 429 без пригодного stale)."""

    def __init__(self, message: str, *, rate_limited: bool = False) -> None:
        super().__init__(message)
        self.rate_limited = rate_limited


_lock = threading.Lock()
# key -> {"stored_wall": float, "expiry_mono": float, "data": dict}
_cache: dict[tuple[Any, ...], dict[str, Any]] = {}

_cycle_stats = {
    "http_requests": 0,
    "cache_hits_fresh": 0,
    "cache_misses": 0,
    "stale_fallback": 0,
    "count_429": 0,
}


def ensemble_cache_ttl_seconds() -> int:
    return max(60, int(os.environ.get("OPENMETEO_ENSEMBLE_CACHE_TTL_SECONDS", str(_DEFAULT_TTL))))


def ensemble_stale_max_seconds() -> int:
    return max(300, int(os.environ.get("OPENMETEO_ENSEMBLE_STALE_MAX_SECONDS", str(_DEFAULT_STALE_MAX))))


def reset_ensemble_cycle_stats() -> None:
    global _cycle_stats
    _cycle_stats = {
        "http_requests": 0,
        "cache_hits_fresh": 0,
        "cache_misses": 0,
        "stale_fallback": 0,
        "count_429": 0,
    }


def get_ensemble_cycle_stats() -> dict[str, int]:
    return dict(_cycle_stats)


def log_ensemble_cycle_stats() -> None:
    s = get_ensemble_cycle_stats()
    log.info(
        "Open-Meteo ensemble round: http_requests=%s cache_hits_fresh=%s cache_misses=%s "
        "stale_fallback=%s http_429=%s (ttl=%ss)",
        s["http_requests"],
        s["cache_hits_fresh"],
        s["cache_misses"],
        s["stale_fallback"],
        s["count_429"],
        ensemble_cache_ttl_seconds(),
    )


def _cache_key(
    city_name: str,
    lat: float,
    lon: float,
    forecast_days: int,
    past_days: int,
    model: str,
) -> tuple[Any, ...]:
    # lat/lon в ключе — если station_config сменится без перезапуска, не смешиваем ответы
    return (city_name, round(lat, 4), round(lon, 4), forecast_days, past_days, model)


def _get_fresh_entry(key: tuple[Any, ...]) -> dict[str, Any] | None:
    now_m = time.monotonic()
    with _lock:
        ent = _cache.get(key)
        if not ent:
            return None
        if now_m < ent["expiry_mono"]:
            return ent["data"]
        return None


def _get_stale_entry(key: tuple[Any, ...]) -> dict[str, Any] | None:
    now_w = time.time()
    stale_max = ensemble_stale_max_seconds()
    with _lock:
        ent = _cache.get(key)
        if not ent:
            return None
        age = now_w - ent["stored_wall"]
        if age <= stale_max:
            return ent["data"]
        return None


def _put_entry(key: tuple[Any, ...], data: dict[str, Any]) -> None:
    ttl = ensemble_cache_ttl_seconds()
    with _lock:
        _cache[key] = {
            "stored_wall": time.time(),
            "expiry_mono": time.monotonic() + ttl,
            "data": data,
        }


def _http_get_ensemble_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    *,
    timeout: int = 30,
    max_retries: int = 2,
) -> dict[str, Any] | None:
    """
    Укороченные повторы при 429 (меньше давления на лимит), чем у общего _http_get_json.
    Возвращает None, если после повторов всё ещё 429 или сеть оборвалась.
    """
    last_exc: BaseException | None = None
    for attempt in range(max_retries):
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                with _lock:
                    _cycle_stats["count_429"] += 1
                wait_s = 2 ** (attempt + 1)
                log.warning(
                    "Open-Meteo ensemble 429 rate limit (attempt %s/%s), sleep %ss",
                    attempt + 1,
                    max_retries,
                    wait_s,
                )
                time.sleep(wait_s)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            last_exc = e
            log.warning("Open-Meteo ensemble HTTP error: %s", e)
            break
    if last_exc:
        log.debug("Open-Meteo ensemble giving up after errors: %s", last_exc)
    return None


def get_ensemble_daily_json_cached(
    *,
    city_name: str,
    lat: float,
    lon: float,
    forecast_days: int,
    past_days: int,
    model: str,
    ensemble_url: str,
    session: requests.Session,
) -> dict[str, Any]:
    """
    Возвращает полный JSON daily ensemble (как от Open-Meteo).
    Считает cache hit/miss, учитывает HTTP в статистике раунда.
    """
    key = _cache_key(city_name, lat, lon, forecast_days, past_days, model)
    fresh = _get_fresh_entry(key)
    if fresh is not None:
        with _lock:
            _cycle_stats["cache_hits_fresh"] += 1
        log.debug("Open-Meteo ensemble cache HIT fresh key=%s", key[:4])
        return fresh

    with _lock:
        _cycle_stats["cache_misses"] += 1
    log.debug("Open-Meteo ensemble cache MISS key=%s", key[:4])

    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max",
        "forecast_days": forecast_days,
        "past_days": past_days,
        "timezone": "UTC",
        "models": model,
    }

    with _lock:
        _cycle_stats["http_requests"] += 1
    data = _http_get_ensemble_json(session, ensemble_url, params, max_retries=2)

    if isinstance(data, dict) and data:
        _put_entry(key, data)
        return data

    stale = _get_stale_entry(key)
    if stale is not None:
        with _lock:
            _cycle_stats["stale_fallback"] += 1
        log.warning(
            "Open-Meteo ensemble: using STALE cached response for %s (after failed fetch / 429)",
            city_name,
        )
        return stale

    raise OpenMeteoEnsembleUnavailable(
        f"Open-Meteo ensemble недоступен для {city_name} (429/ошибка сети, нет кэша)",
        rate_limited=True,
    )
