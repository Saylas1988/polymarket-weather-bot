"""
Конфигурация городов Polymarket «Highest temperature» ↔ resolving station / источник.

Координаты station_lat/station_lon — точка Open‑Meteo ECMWF ensemble (приближение к станции
резолва Polymarket). Polymarket для большинства городов указывает Wunderground; для Москвы — NOAA.
Прогноз модели ≠ фактический WU/NOAA; этап 1 только выравнивает геометрию с аэропортом.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

SourceType = Literal["wunderground", "noaa"]
CityMode = Literal["disabled", "paper_only", "live_candidate"]


@dataclass(frozen=True)
class CityStationConfig:
    city_key: str
    display_name_en: str
    display_name_ru: str
    source_type: SourceType
    station_code: str
    station_name: str
    station_lat: float
    station_lon: float
    resolution_url: str
    mode: CityMode


# Режимы: на этапе 1 все города — paper_only (disabled / live_candidate зарезервированы).
CITY_STATIONS: tuple[CityStationConfig, ...] = (
    CityStationConfig(
        city_key="london",
        display_name_en="London",
        display_name_ru="Лондон",
        source_type="wunderground",
        station_code="EGLC",
        station_name="London City Airport",
        station_lat=51.5048,
        station_lon=0.0495,
        resolution_url="https://www.wunderground.com/history/daily/gb/london/EGLC",
        mode="paper_only",
    ),
    CityStationConfig(
        city_key="paris",
        display_name_en="Paris",
        display_name_ru="Париж",
        source_type="wunderground",
        station_code="LFPG",
        station_name="Charles de Gaulle Airport",
        station_lat=49.0097,
        station_lon=2.5479,
        resolution_url="https://www.wunderground.com/history/daily/fr/paris/LFPG",
        mode="paper_only",
    ),
    CityStationConfig(
        city_key="munich",
        display_name_en="Munich",
        display_name_ru="Мюнхен",
        source_type="wunderground",
        station_code="EDDM",
        station_name="Munich Airport",
        station_lat=48.3538,
        station_lon=11.7861,
        resolution_url="https://www.wunderground.com/history/daily/de/munich/EDDM",
        mode="paper_only",
    ),
    CityStationConfig(
        city_key="milan",
        display_name_en="Milan",
        display_name_ru="Милан",
        source_type="wunderground",
        station_code="LIMC",
        station_name="Malpensa Airport",
        station_lat=45.6306,
        station_lon=8.7281,
        resolution_url="https://www.wunderground.com/history/daily/it/milan/LIMC",
        mode="paper_only",
    ),
    CityStationConfig(
        city_key="madrid",
        display_name_en="Madrid",
        display_name_ru="Мадрид",
        source_type="wunderground",
        station_code="LEMD",
        station_name="Adolfo Suárez Madrid-Barajas Airport",
        station_lat=40.4983,
        station_lon=-3.5676,
        resolution_url="https://www.wunderground.com/history/daily/es/madrid/LEMD",
        mode="paper_only",
    ),
    CityStationConfig(
        city_key="warsaw",
        display_name_en="Warsaw",
        display_name_ru="Варшава",
        source_type="wunderground",
        station_code="EPWA",
        station_name="Warsaw Chopin Airport",
        station_lat=52.1657,
        station_lon=20.9671,
        resolution_url="https://www.wunderground.com/history/daily/pl/warsaw/EPWA",
        mode="paper_only",
    ),
    CityStationConfig(
        city_key="moscow",
        display_name_en="Moscow",
        display_name_ru="Москва",
        source_type="noaa",
        station_code="UUWW",
        station_name="Vnukovo International Airport",
        station_lat=55.5915,
        station_lon=37.2615,
        resolution_url="https://www.weather.gov/wrh/timeseries?site=UUWW",
        mode="paper_only",
    ),
    CityStationConfig(
        city_key="ankara",
        display_name_en="Ankara",
        display_name_ru="Анкара",
        source_type="wunderground",
        station_code="LTAC",
        station_name="Esenboğa Airport",
        station_lat=40.1281,
        station_lon=32.9951,
        resolution_url="https://www.wunderground.com/history/daily/tr/%C3%A7ubuk/LTAC",
        mode="paper_only",
    ),
    CityStationConfig(
        city_key="helsinki",
        display_name_en="Helsinki",
        display_name_ru="Хельсинки",
        source_type="wunderground",
        station_code="EFHK",
        station_name="Helsinki Vantaa Airport",
        station_lat=60.3172,
        station_lon=24.9633,
        resolution_url="https://www.wunderground.com/history/daily/fi/vantaa/EFHK",
        mode="paper_only",
    ),
)


def city_config_by_display_name(name: str) -> CityStationConfig:
    for c in CITY_STATIONS:
        if c.display_name_en == name:
            return c
    raise AssertionError(f"Неизвестный город (нет в station_config): {name}")


def city_config_from_slug_token(city_slug: str) -> CityStationConfig:
    """city_slug — фрагмент из event slug: london, paris, …"""
    cs = city_slug.strip().lower()
    for c in CITY_STATIONS:
        if c.city_key == cs:
            return c
    raise AssertionError(f"Город из slug не в station_config: {city_slug}")


def iter_enabled_city_configs() -> tuple[CityStationConfig, ...]:
    """Города, участвующие в раундах сигналов (не disabled)."""
    return tuple(c for c in CITY_STATIONS if c.mode != "disabled")


def is_city_trading_enabled(cfg: CityStationConfig) -> bool:
    return cfg.mode != "disabled"
