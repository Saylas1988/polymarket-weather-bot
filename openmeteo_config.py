"""
Open-Meteo: free public endpoints vs Professional (customer) API.

Документация: https://open-meteo.com/en/pricing
- customer API: хост с префиксом customer- и параметр &apikey=...
- синтаксис запросов совпадает с бесплатным API.

Переменная окружения: OPENMETEO_API_KEY — если задана (непустая), используются
customer-api.open-meteo.com и customer-ensemble-api.open-meteo.com и apikey в query.
"""

from __future__ import annotations

import logging
import os
from typing import Any

log = logging.getLogger("rainmaker")

_logged_mode = False

# Free tier (публичные инстансы)
_ECMWF_FREE = "https://api.open-meteo.com/v1/ecmwf"
_ENSEMBLE_FREE = "https://ensemble-api.open-meteo.com/v1/ensemble"

# Professional / customer (см. pricing & docs ensemble/ecmwf)
_ECMWF_CUSTOMER = "https://customer-api.open-meteo.com/v1/ecmwf"
_ENSEMBLE_CUSTOMER = "https://customer-ensemble-api.open-meteo.com/v1/ensemble"

# Historical Weather (Archive API) — дневные агрегаты для верификации итога рынка.
# См. https://open-meteo.com/en/docs/historical-weather-api
# При OPENMETEO_API_KEY добавляется &apikey=... (как для ECMWF/ensemble).
_ARCHIVE_FREE = "https://archive-api.open-meteo.com/v1/archive"


def openmeteo_api_key() -> str:
    """Ключ Professional API; только из окружения, без дефолта в коде."""
    return os.environ.get("OPENMETEO_API_KEY", "").strip()


def is_openmeteo_paid_mode() -> bool:
    return bool(openmeteo_api_key())


def ecmwf_base_url() -> str:
    return _ECMWF_CUSTOMER if is_openmeteo_paid_mode() else _ECMWF_FREE


def ensemble_base_url() -> str:
    return _ENSEMBLE_CUSTOMER if is_openmeteo_paid_mode() else _ENSEMBLE_FREE


def archive_base_url() -> str:
    """
    Historical Weather API (daily archive). По умолчанию публичный archive-api.open-meteo.com;
    с OPENMETEO_API_KEY к запросу добавляется apikey (см. merge_openmeteo_auth).
    Переопределение: OPENMETEO_ARCHIVE_BASE_URL — полный URL до /v1/archive включительно.
    """
    raw = os.environ.get("OPENMETEO_ARCHIVE_BASE_URL", "").strip()
    if raw:
        return raw.rstrip("/")
    return _ARCHIVE_FREE


def merge_openmeteo_auth(params: dict[str, Any]) -> dict[str, Any]:
    """Добавляет apikey к параметрам GET, если задан OPENMETEO_API_KEY."""
    out = dict(params)
    key = openmeteo_api_key()
    if key:
        out["apikey"] = key
    return out


def openmeteo_mode_label() -> str:
    return "paid_professional" if is_openmeteo_paid_mode() else "free_public"


def log_openmeteo_mode_once() -> None:
    """Один раз за процесс: режим и маскированный хвост ключа (не логировать полный ключ)."""
    global _logged_mode
    if _logged_mode:
        return
    _logged_mode = True
    key = openmeteo_api_key()
    if key:
        tail = key[-4:] if len(key) >= 4 else "****"
        log.info(
            "Open-Meteo: Professional customer hosts "
            "(customer-api.open-meteo.com, customer-ensemble-api.open-meteo.com), "
            "apikey present (suffix …%s)",
            tail,
        )
    else:
        log.info(
            "Open-Meteo: public/free endpoints (set OPENMETEO_API_KEY for Professional customer API)"
        )
