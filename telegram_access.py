"""
Ограничение входящих команд Telegram: только разрешённый chat и/или user.

Исходящие сообщения (сигналы, paper, отчёты) не затрагиваются — они используют
TELEGRAM_CHAT_ID в send_message напрямую.

Переменные окружения (опционально, любая комбинация):
- ALLOWED_TELEGRAM_CHAT_ID — числовой id чата (private: совпадает с user id собеседника)
- ALLOWED_TELEGRAM_USER_ID — числовой id пользователя

Если обе заданы: должны совпасть обе проверки.
Если только одна: достаточно совпадения по ней.
Если ни одна не задана — контроль выключен (обратная совместимость).
"""

from __future__ import annotations

import logging
import os
from typing import Any

log = logging.getLogger("rainmaker")

_access_mode_logged = False


def _parse_int_env(name: str) -> int | None:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        log.warning("telegram_access: %s=%r не число, игнор", name, raw[:20])
        return None


def allowed_telegram_chat_id() -> int | None:
    return _parse_int_env("ALLOWED_TELEGRAM_CHAT_ID")


def allowed_telegram_user_id() -> int | None:
    return _parse_int_env("ALLOWED_TELEGRAM_USER_ID")


def telegram_access_control_enabled() -> bool:
    return allowed_telegram_chat_id() is not None or allowed_telegram_user_id() is not None


def log_telegram_access_mode_once() -> None:
    global _access_mode_logged
    if _access_mode_logged:
        return
    _access_mode_logged = True
    if not telegram_access_control_enabled():
        log.info(
            "Telegram access control: disabled (incoming commands open; "
            "set ALLOWED_TELEGRAM_CHAT_ID and/or ALLOWED_TELEGRAM_USER_ID to restrict)"
        )
        return
    parts: list[str] = []
    c = allowed_telegram_chat_id()
    u = allowed_telegram_user_id()
    if c is not None:
        parts.append(f"require chat_id={c}")
    if u is not None:
        parts.append(f"require user_id={u}")
    log.info("Telegram access control: enabled (%s)", "; ".join(parts))


def is_incoming_telegram_allowed(update: Any) -> bool:
    """
    True — обработать команду. False — молча игнорировать.
    """
    if not telegram_access_control_enabled():
        return True
    want_chat = allowed_telegram_chat_id()
    want_user = allowed_telegram_user_id()

    chat = getattr(update, "effective_chat", None)
    user = getattr(update, "effective_user", None)
    cid = chat.id if chat is not None else None
    uid = user.id if user is not None else None

    chat_ok = want_chat is None or (cid is not None and cid == want_chat)
    user_ok = want_user is None or (uid is not None and uid == want_user)
    return bool(chat_ok and user_ok)


def log_if_telegram_blocked(update: Any) -> None:
    """Один debug-лог на отброшенное входящее (без секретов)."""
    chat = getattr(update, "effective_chat", None)
    user = getattr(update, "effective_user", None)
    cid = chat.id if chat is not None else None
    uid = user.id if user is not None else None
    log.debug("telegram access: dropped incoming (chat_id=%s user_id=%s)", cid, uid)
