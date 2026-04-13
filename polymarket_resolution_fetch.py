"""
Извлечение фактической дневной максимальной температуры в духе resolving source Polymarket.

- Wunderground (source_type=wunderground): сначала Weather Company API (тот же стек, что у WU —
  v3/wx/conditions/historical/dailysummary/30day по geocode станции), при недоступности окна —
  METAR по ICAO через Ogimet (аэропорт = станция в конфиге).

- NOAA (source_type=noaa): METAR по ICAO через Ogimet — наблюдения у метеостанции аэропорта;
  это не HTML weather.gov, но тот же класс наблюдений, что используют авиационные/оперативные
  сводки; HTML NWS без стабильного JSON для UUWW машинно не разобран.

Ограничения честно отражаются в полях verification_method / verification_note на уровне caller.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import re
from dataclasses import dataclass
from typing import Any

import requests
from zoneinfo import ZoneInfo

from station_config import CityStationConfig

log = logging.getLogger("rainmaker")

WEATHERCOM_REFERER = "https://www.wunderground.com/"

# Публичный ключ из разметки WU (как в странице history). Можно заменить через env.
DEFAULT_WEATHERCOM_API_KEY = "e1f10a1e78da46f5b10a1e78da96f525"


def weathercom_api_key() -> str:
    return os.environ.get("WEATHERCOM_API_KEY", DEFAULT_WEATHERCOM_API_KEY).strip()


@dataclass
class TemperatureFetchResult:
    verified_temperature_c: float | None
    source_of_truth: str
    verification_method: str
    detail: dict[str, Any]
    verification_note: str


def _parse_metar_temperature_c(report: str) -> float | None:
    """
    Достаёт температуру воздуха (°C) из тела METAR/SPECI: группа TT/dd перед Q.... или в конце.
    Примеры: 20/11, M05/M07, 10/07
    """
    report = report.strip().replace("=", " ")
    m = re.search(r"(?<![\w/])(M?\d{2})/(M?\d{2})(?=\s+Q\d)", report)
    if not m:
        m = re.search(r"\s(M?\d{2})/(M?\d{2})\s*$", report)
    if not m:
        return None

    def tok(s: str) -> float:
        s = s.strip()
        if s.startswith("M"):
            return -float(s[1:])
        return float(s)

    try:
        return tok(m.group(1))
    except ValueError:
        return None


def ogimet_metar_daily_max_c(
    icao: str,
    event_date: dt.date,
    tz_name: str,
) -> tuple[float | None, dict[str, Any]]:
    """Максимум температуры по METAR Ogimet за локальный календарный день."""
    z = ZoneInfo(tz_name)
    start = dt.datetime.combine(event_date, dt.time.min, tzinfo=z)
    end = dt.datetime.combine(event_date, dt.time(23, 59), tzinfo=z)
    su = start.astimezone(dt.timezone.utc)
    eu = end.astimezone(dt.timezone.utc)
    begin = su.strftime("%Y%m%d%H%M")
    endp = eu.strftime("%Y%m%d%H%M")
    url = "https://www.ogimet.com/cgi-bin/getmetar"
    params = {"icao": icao.strip().upper(), "begin": begin, "end": endp}
    detail: dict[str, Any] = {
        "provider": "ogimet",
        "icao": icao.upper(),
        "begin": begin,
        "end": endp,
    }
    try:
        r = requests.get(
            url,
            params=params,
            timeout=45,
            headers={"User-Agent": "RainMakerBot/1.0 (polymarket verification; +https://polymarket.com)"},
        )
        r.raise_for_status()
        text = r.content.decode("iso-8859-1", errors="replace")
    except Exception as e:
        detail["error"] = str(e)
        return None, detail

    temps: list[float] = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(",", 6)
        if len(parts) < 7:
            continue
        report = parts[6].strip()
        t = _parse_metar_temperature_c(report)
        if t is not None:
            temps.append(t)

    detail["metar_count"] = len(temps)
    if not temps:
        detail["error"] = "no_metar_temperatures_parsed"
        return None, detail
    mx = max(temps)
    detail["max_from_metar_c"] = mx
    return float(mx), detail


def weathercom_30day_daily_max_for_date_c(
    lat: float,
    lon: float,
    event_date: dt.date,
    tz_name: str,
) -> tuple[float | None, dict[str, Any]]:
    """
    IBM / Weather Company historical dailysummary (30 дней), как у страниц WU.
    Требует Referer wunderground.com; ключ — как у встраиваемого клиента WU.
    """
    detail: dict[str, Any] = {"provider": "weather_com", "endpoint": "dailysummary/30day"}
    key = weathercom_api_key()
    if not key:
        detail["error"] = "missing_WEATHERCOM_API_KEY"
        return None, detail

    url = "https://api.weather.com/v3/wx/conditions/historical/dailysummary/30day"
    geocode = f"{lat},{lon}"
    params = {
        "apiKey": key,
        "geocode": geocode,
        "units": "m",
        "language": "en-US",
        "format": "json",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; RainMakerBot/1.0)",
        "Referer": WEATHERCOM_REFERER,
    }
    try:
        r = requests.get(url, params=params, timeout=45, headers=headers)
        r.raise_for_status()
        payload = r.json()
    except Exception as e:
        detail["error"] = str(e)
        return None, detail

    times = payload.get("validTimeLocal") or []
    tmaxs = payload.get("temperatureMax") or []
    if not isinstance(times, list) or not isinstance(tmaxs, list):
        detail["error"] = "unexpected_payload_shape"
        return None, detail

    for tstr, tx in zip(times, tmaxs):
        if tx is None:
            continue
        try:
            # "2026-04-13T07:00:00+0100"
            tss = str(tstr)
            if len(tss) >= 10:
                y, mo, da = int(tss[0:4]), int(tss[5:7]), int(tss[8:10])
                d = dt.date(y, mo, da)
                if d == event_date:
                    detail["matched_validTimeLocal"] = tstr
                    return float(tx), detail
        except (ValueError, TypeError):
            continue

    detail["error"] = "date_not_in_30day_window"
    return None, detail


def fetch_polymarket_resolution_temperature(
    cfg: CityStationConfig,
    *,
    event_date: dt.date,
    tz_name: str,
    local_today: dt.date,
) -> TemperatureFetchResult:
    """
    Главная точка входа: выбирает цепочку по source_type и давности даты.
    """
    lat, lon = float(cfg.station_lat), float(cfg.station_lon)
    icao = cfg.station_code.strip().upper()
    days_ago = (local_today - event_date).days

    # --- NOAA (сейчас только Москва UUWW): METAR Ogimet как наблюдения на станции ---
    if cfg.source_type == "noaa":
        t, det = ogimet_metar_daily_max_c(icao, event_date, tz_name)
        if t is not None:
            return TemperatureFetchResult(
                verified_temperature_c=t,
                source_of_truth="NOAA ecosystem (METAR/Ogimet)",
                verification_method="noaa_metar_ogimet_daily_max",
                detail=det,
                verification_note=(
                    "Дневной максимум по METAR ICAO из Ogimet за локальный день города; "
                    "соответствует наблюдениям на аэродроме. HTML weather.gov timeseries без "
                    "стабильного API для автоматизации не используется."
                ),
            )
        return TemperatureFetchResult(
            verified_temperature_c=None,
            source_of_truth="NOAA (unavailable)",
            verification_method="noaa_metar_ogimet_failed",
            detail=det,
            verification_note="Не удалось получить METAR Ogimet для станции.",
        )

    # --- Wunderground: Weather Company 30d, иначе METAR как запасной путь той же ICAO станции ---
    if cfg.source_type == "wunderground":
        if 0 <= days_ago <= 29:
            t, det = weathercom_30day_daily_max_for_date_c(lat, lon, event_date, tz_name)
            if t is not None:
                return TemperatureFetchResult(
                    verified_temperature_c=t,
                    source_of_truth="Wunderground / Weather Company",
                    verification_method="wunderground_weather_com_dailysummary_30day",
                    detail=det,
                    verification_note=(
                        "Исторический дневной max из Weather Company API (тот же стек, что у WU), "
                        "geocode = станция из station_config; validTimeLocal сопоставляется с датой события."
                    ),
                )
            log.info(
                "WU verify: Weather.com не вернул дату %s (%s), пробуем Ogimet METAR",
                event_date.isoformat(),
                det.get("error"),
            )

        t2, det2 = ogimet_metar_daily_max_c(icao, event_date, tz_name)
        if t2 is not None:
            return TemperatureFetchResult(
                verified_temperature_c=t2,
                source_of_truth="Wunderground station (METAR/Ogimet fallback)",
                verification_method="wunderground_ogimet_metar_daily_max_fallback",
                detail=det2,
                verification_note=(
                    "Окно Weather.com 30day не содержит дату события или API не сработал; "
                    "использован максимум температуры по METAR на ICAO станции Polymarket (Ogimet). "
                    "Обычно совпадает с дневным high по станции; расхождение с округлением WU возможно."
                ),
            )

        return TemperatureFetchResult(
            verified_temperature_c=None,
            source_of_truth="Wunderground (unavailable)",
            verification_method="wunderground_all_sources_failed",
            detail=det2,
            verification_note="Weather.com и Ogimet METAR не дали tmax.",
        )

    return TemperatureFetchResult(
        verified_temperature_c=None,
        source_of_truth="unknown",
        verification_method="unsupported_source_type",
        detail={},
        verification_note=f"Неизвестный source_type: {cfg.source_type}",
    )
