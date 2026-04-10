"""
Журнал сигналов (JSON Lines): прошедшие и непрошедшие проверки.
Ошибка записи не должна ронять основной поток — перехватываем всё и логируем.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from zoneinfo import ZoneInfo

log = logging.getLogger("rainmaker")


def append_signal_journal_record(record: dict[str, Any], *, path: str) -> None:
    """Добавить одну строку JSON в конец файла."""
    try:
        line = json.dumps(record, ensure_ascii=False, default=str) + "\n"
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception as e:
        log.warning("signal journal write failed: %s", e, exc_info=False)


def utc_now_iso() -> str:
    return datetime.now(ZoneInfo("UTC")).replace(microsecond=0).isoformat()


def msk_now_iso() -> str:
    return datetime.now(ZoneInfo("Europe/Moscow")).replace(microsecond=0).isoformat()
