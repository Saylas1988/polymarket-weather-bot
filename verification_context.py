"""
Заготовка под future verification layer (NOAA/WU/post-resolution).

Сейчас только собирает структурированный контекст без внешних HTTP-запросов.
Подключение фактических проверок — на следующих этапах.
"""

from __future__ import annotations

from typing import Any


def build_resolution_context(
    *,
    city_key: str,
    station_code: str,
    source_type: str,
    resolution_url: str,
    event_date: str,
    event_slug: str,
    display_name_en: str,
    station_lat: float,
    station_lon: float,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Контекст для будущего сравнения прогноза/сигнала с фактическим резолвом Polymarket.

    Поля совпадают с тем, что пишется в signal journal, чтобы offline можно было
    сопоставить запись с источником истины (WU/NOAA и т.д.).
    """
    ctx: dict[str, Any] = {
        "version": 1,
        "city_key": city_key,
        "display_name_en": display_name_en,
        "station_code": station_code,
        "source_type": source_type,
        "resolution_url": resolution_url,
        "event_date": event_date,
        "event_slug": event_slug,
        "station_lat": station_lat,
        "station_lon": station_lon,
        "notes": (
            "station metadata для сопоставления с правилами Polymarket; фактическая верификация итога — "
            "см. market_outcome_verify (Open-Meteo Archive) + paper_verification_state.json"
        ),
    }
    if extra:
        ctx["extra"] = extra
    return ctx
