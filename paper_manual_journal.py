"""
Текстовый автожурнал «что купил по боту» в простом формате (дополнение к JSONL).

Пишется только при фактическом открытии paper-позиции; дедупликация по paper_trade_id.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any

from paper_settings import paper_manual_journal_path

log = logging.getLogger("rainmaker")

_lock = threading.Lock()


def _dedupe_marker(trade_id: str) -> str:
    return f"# dedupe_id={trade_id}"


def _verification_marker(event_slug: str) -> str:
    return f"# verification_id={event_slug}"


def _already_logged(path: str, trade_id: str) -> bool:
    marker = _dedupe_marker(trade_id)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return marker in f.read()
    except FileNotFoundError:
        return False
    except OSError as e:
        log.warning("paper_manual_journal: не удалось проверить дубликат: %s", e)
        return False


def _verification_already_logged(path: str, event_slug: str) -> bool:
    marker = _verification_marker(event_slug)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return marker in f.read()
    except FileNotFoundError:
        return False
    except OSError as e:
        log.warning("paper_manual_journal: не удалось проверить verification duplicate: %s", e)
        return False


def _format_entry_block(pos: dict[str, Any], res: dict[str, Any] | None) -> str:
    city = str(pos.get("display_name_ru") or pos.get("display_name_en") or "?")
    ed = str(pos.get("event_date") or "?")
    depth = pos.get("depth")
    if depth is not None:
        try:
            dstr = f"D+{int(depth)}"
        except (TypeError, ValueError):
            dstr = f"D+{depth}"
    else:
        dstr = "?"

    slug = str(pos.get("event_slug") or "")
    url = str(pos.get("event_url") or (f"https://polymarket.com/event/{slug}" if slug else ""))

    tid = str(pos.get("paper_trade_id") or "")

    lines: list[str] = [
        "====================",
        f"Город: {city}",
        f"Дата события: {ed}",
        f"Горизонт: {dstr}",
        "",
        f"Событие: {slug}",
        f"Рынок: {url}",
        "",
        "Купил по боту:",
    ]

    for leg in pos.get("legs") or []:
        if not isinstance(leg, dict):
            continue
        lbl = str(leg.get("bucket_label") or leg.get("leg_key") or "?")
        usd = leg.get("allocated_usd")
        ep = leg.get("entry_price_assumed")
        tg = leg.get("target_sell_price")
        usd_s = f"${float(usd):.2f}" if isinstance(usd, (int, float)) else str(usd)
        ep_s = f"{float(ep):.2f}" if isinstance(ep, (int, float)) else "?"
        tg_s = f"{float(tg):.2f}" if isinstance(tg, (int, float)) else "?"
        lines.append(f"- {lbl}: {usd_s} · entry~{ep_s} · цель {tg_s}")

    lines.append("")
    lines.append("Сводка entry (по ногам, assumed):")
    eparts: list[str] = []
    for leg in pos.get("legs") or []:
        if not isinstance(leg, dict):
            continue
        lk = str(leg.get("leg_key") or "?")
        ep = leg.get("entry_price_assumed")
        eparts.append(f"{lk}~{float(ep):.2f}" if isinstance(ep, (int, float)) else f"{lk}=?")
    lines.append(" · ".join(eparts) if eparts else "—")

    lines.append("")
    lines.append("Цель (virtual limit / target sell):")
    tparts: list[str] = []
    for leg in pos.get("legs") or []:
        if not isinstance(leg, dict):
            continue
        lk = str(leg.get("leg_key") or "?")
        tg = leg.get("target_sell_price")
        tparts.append(f"{lk}->{float(tg):.2f}" if isinstance(tg, (int, float)) else f"{lk}=?")
    lines.append(" · ".join(tparts) if tparts else "—")

    if isinstance(res, dict):
        pm = res.get("p_main")
        if isinstance(pm, (int, float)):
            lines.append("")
            lines.append(f"Ensemble p(main) на входе: {float(pm) * 100:.0f}%")

    lines.append("")
    lines.append("Комментарий:")
    lines.append("")
    if tid:
        lines.append(_dedupe_marker(tid))
    lines.append("====================")
    lines.append("")
    return "\n".join(lines)


def append_paper_manual_journal_entry(pos: dict[str, Any], res: dict[str, Any] | None = None) -> None:
    """Дописать блок при открытии позиции; при повторном том же paper_trade_id — ничего не делать."""
    tid = str(pos.get("paper_trade_id") or "").strip()
    if not tid:
        log.warning("paper_manual_journal: нет paper_trade_id, пропуск")
        return

    path = os.path.abspath(paper_manual_journal_path())
    parent = os.path.dirname(path)
    block = _format_entry_block(pos, res)

    with _lock:
        if _already_logged(path, tid):
            log.debug("paper_manual_journal: запись для dedupe_id=%s уже есть, пропуск", tid[:8])
            return
        try:
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(path, "a", encoding="utf-8", newline="\n") as f:
                f.write(block)
        except OSError as e:
            log.warning("paper_manual_journal: не удалось записать %s: %s", path, e)


def append_market_verification_block(
    *,
    event_slug: str,
    winning_bucket_label: str,
    verified_temperature_c: float,
    verification_method: str,
    resolution_hint: str,
    ladder_unit: str,
    observed_in_market_unit: float,
    source_of_truth: str,
) -> bool:
    """
    Дописать блок итога рынка; дедуп по # verification_id=<event_slug>.
    observed_in_market_unit — факт в единицах лестницы (C или F), как в сигналах.
    """
    slug = str(event_slug or "").strip()
    if not slug:
        log.warning("paper_manual_journal: verification без event_slug, пропуск")
        return False

    path = os.path.abspath(paper_manual_journal_path())
    parent = os.path.dirname(path)

    u = str(ladder_unit or "C").upper()
    unit_s = "°C" if u == "C" else "°F"
    lines = [
        "--------------------",
        "Итог на рынке (автоверификация)",
        f"Событие: {slug}",
        f"Победил: {winning_bucket_label}",
        f"Факт {unit_s}: {float(observed_in_market_unit):.2f}",
        f"Факт °C: {float(verified_temperature_c):.2f}",
        f"Source of truth: {source_of_truth}",
        f"Метод: {verification_method}",
        f"Подсказка резолва Polymarket: {resolution_hint}",
        _verification_marker(slug),
        "--------------------",
        "",
    ]
    block = "\n".join(lines)

    with _lock:
        if _verification_already_logged(path, slug):
            log.debug("paper_manual_journal: verification для %s уже есть, пропуск", slug[:48])
            return True
        try:
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(path, "a", encoding="utf-8", newline="\n") as f:
                f.write(block)
        except OSError as e:
            log.warning("paper_manual_journal: не удалось записать verification %s: %s", path, e)
            return False
    return True
