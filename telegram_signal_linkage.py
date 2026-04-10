"""
Связь event_slug → корневое сообщение Telegram (сигнал) для reply-цепочки paper-событий.
Локальный JSON рядом с runtime-файлами; без внешней БД.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from typing import Any

log = logging.getLogger("rainmaker")

_LINKAGE_FILE = os.path.join(os.path.dirname(__file__), "telegram_signal_linkage.json")


def _max_age_days() -> int:
    return max(7, int(os.environ.get("TELEGRAM_SIGNAL_LINKAGE_MAX_AGE_DAYS", "90")))


def _max_entries() -> int:
    return max(50, int(os.environ.get("TELEGRAM_SIGNAL_LINKAGE_MAX_ENTRIES", "400")))


def _load() -> dict[str, Any]:
    try:
        with open(_LINKAGE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception as e:
        log.warning("telegram_signal_linkage: не удалось прочитать %s: %s", _LINKAGE_FILE, e)
        return {}


def _atomic_write(path: str, obj: dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _parse_iso_utc(s: str) -> dt.datetime | None:
    try:
        x = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if x.tzinfo is None:
            x = x.replace(tzinfo=dt.timezone.utc)
        return x.astimezone(dt.timezone.utc)
    except (ValueError, TypeError):
        return None


def _prune(by_slug: dict[str, Any]) -> dict[str, Any]:
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=_max_age_days())
    out: dict[str, Any] = {}
    for slug, rec in by_slug.items():
        if not isinstance(rec, dict):
            continue
        at = rec.get("at_utc")
        if not isinstance(at, str):
            continue
        parsed = _parse_iso_utc(at)
        if parsed is None or parsed < cutoff:
            continue
        out[str(slug)] = rec

    max_n = _max_entries()
    if len(out) <= max_n:
        return out

    ranked = []
    for slug, rec in out.items():
        p = _parse_iso_utc(str(rec.get("at_utc") or ""))
        ranked.append((p or dt.datetime.min.replace(tzinfo=dt.timezone.utc), slug, rec))
    ranked.sort(key=lambda x: x[0], reverse=True)
    trimmed = {slug: rec for _, slug, rec in ranked[:max_n]}
    if len(trimmed) < len(out):
        log.info(
            "telegram_signal_linkage: усечено до %s записей (лимит TELEGRAM_SIGNAL_LINKAGE_MAX_ENTRIES)",
            max_n,
        )
    return trimmed


def record_signal_message(event_slug: str, chat_id: str, message_id: int, at_utc: dt.datetime) -> None:
    """Сохранить/обновить корневое сообщение сигнала для event_slug."""
    data = _load()
    data.setdefault("version", 1)
    by_slug = data.setdefault("by_slug", {})
    if not isinstance(by_slug, dict):
        by_slug = {}
        data["by_slug"] = by_slug

    by_slug[str(event_slug)] = {
        "chat_id": str(chat_id).strip(),
        "message_id": int(message_id),
        "at_utc": at_utc.replace(microsecond=0).isoformat(),
    }
    data["by_slug"] = _prune(by_slug)
    try:
        _atomic_write(_LINKAGE_FILE, data)
    except Exception as e:
        log.warning("telegram_signal_linkage: не удалось записать: %s", e)


def get_signal_thread_root(event_slug: str) -> dict[str, Any] | None:
    """
    Вернуть {chat_id, message_id, at_utc} для reply на корневой сигнал, или None.
    """
    data = _load()
    by_slug = data.get("by_slug")
    if not isinstance(by_slug, dict):
        return None
    rec = by_slug.get(str(event_slug))
    if not isinstance(rec, dict):
        return None
    mid = rec.get("message_id")
    cid = rec.get("chat_id")
    if mid is None or cid is None:
        return None
    try:
        return {
            "chat_id": str(cid).strip(),
            "message_id": int(mid),
            "at_utc": rec.get("at_utc"),
        }
    except (TypeError, ValueError):
        return None
