"""
Короткие тексты для Telegram: paper trading (вход, выход, сводки, /paper).
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from typing import Any

from zoneinfo import ZoneInfo

from paper_portfolio import load_portfolio
from paper_settings import (
    allocation_logic_version,
    exit_logic_version,
    fee_logic_version,
    paper_start_balance,
    paper_trading_enabled,
)


def _msk_date_from_utc_iso(iso: str | None) -> date | None:
    if not iso or not isinstance(iso, str):
        return None
    try:
        u = iso.replace("Z", "+00:00")
        dtu = datetime.fromisoformat(u)
        if dtu.tzinfo is None:
            dtu = dtu.replace(tzinfo=ZoneInfo("UTC"))
        return dtu.astimezone(ZoneInfo("Europe/Moscow")).date()
    except Exception:
        return None


def _target_line(targets: dict[str, Any] | None) -> str:
    if not targets:
        return "targets: —"
    parts = [f"{k}={float(v):.3f}" for k, v in targets.items() if isinstance(v, (int, float))]
    return "targets: " + ", ".join(parts[:6]) if parts else "targets: —"


def format_paper_entry_message(
    *,
    display_name_en: str,
    event_slug: str,
    station_code: str | None,
    depth: int | None,
    structure_type_requested: str,
    structure_type_effective: str,
    total_allocated_usd: float,
    cash_before: float,
    cash_after: float,
    allocator_notes: list[str] | None,
    neighbor_cuts: list[str] | None,
    target_summary: dict[str, Any] | None,
) -> str:
    notes = allocator_notes or []
    cuts = neighbor_cuts or []
    alloc_brief = eff = structure_type_effective
    if notes:
        alloc_brief += f" | {notes[0]}"
    if cuts:
        alloc_brief += f" | cuts:{len(cuts)}"
    lines = [
        "📌 PAPER ENTRY",
        f"{display_name_en} | D+{depth if depth is not None else '?'} | st {station_code or '—'}",
        f"event: {event_slug}",
        f"structure: req {structure_type_requested} → eff {eff}",
        f"alloc: ${total_allocated_usd:.2f} | cash ${cash_before:.2f} → ${cash_after:.2f}",
        f"allocator: {alloc_brief}",
        _target_line(target_summary),
    ]
    return "\n".join(lines)


def format_paper_leg_exit_message(
    *,
    display_name_en: str,
    event_slug: str,
    leg_key: str | None,
    bucket_label: str | None,
    exit_reason: str,
    leg_realized_pnl: float,
    cash_after: float,
) -> str:
    leg = leg_key or "?"
    lbl = bucket_label or ""
    extra = f" ({lbl})" if lbl else ""
    lines = [
        "📤 PAPER EXIT (leg)",
        f"{display_name_en} | {event_slug}",
        f"leg: {leg}{extra}",
        f"reason: {exit_reason}",
        f"leg PnL: ${leg_realized_pnl:+.4f} | cash now ${cash_after:.2f}",
    ]
    return "\n".join(lines)


def format_paper_position_closed_message(
    *,
    display_name_en: str,
    event_slug: str,
    exit_kind: str,
    realized_pnl: float,
    cash_after: float,
    extra_note: str | None = None,
) -> str:
    lines = [
        "🏁 PAPER EXIT (position)",
        f"{display_name_en} | {event_slug}",
        f"kind: {exit_kind}",
        f"realized PnL: ${realized_pnl:+.4f} | cash now ${cash_after:.2f}",
    ]
    if extra_note:
        lines.append(extra_note)
    return "\n".join(lines)


def _pnl_by_city_on_msk_date(portfolio: dict[str, Any], d: date) -> dict[str, float]:
    out: dict[str, float] = {}
    for c in portfolio.get("closed_positions") or []:
        if not isinstance(c, dict):
            continue
        cdt = _msk_date_from_utc_iso(c.get("closed_at_utc"))
        if cdt != d:
            continue
        k = str(c.get("city_key") or "?")
        out[k] = out.get(k, 0.0) + float(c.get("realized_pnl") or 0)
    return out


def format_daily_telegram_summary(for_day_msk: date | None = None) -> str:
    """Сводка за календарный день МСК for_day_msk (по умолчанию сегодня МСК)."""
    msk = ZoneInfo("Europe/Moscow")
    d = for_day_msk or datetime.now(msk).date()
    port = load_portfolio()
    cash = float(port.get("current_cash") or 0)
    realized = float(port.get("realized_pnl") or 0)
    unreal = float(port.get("unrealized_pnl_estimate") or 0)
    open_n = len(port.get("open_positions") or {})
    st = port.get("stats") or {}
    se = st.get("structure_entries") or {}
    prev = st.get("paper_prev_msk_summary") or {}
    # активность за запрошенный день: из архива при смене суток
    if isinstance(prev, dict) and prev.get("date") == d.isoformat():
        ent = int(prev.get("entries") or 0)
        ex = int(prev.get("exits") or 0)
        sk = int(prev.get("skipped") or 0)
    elif st.get("paper_activity_date_msk") == d.isoformat():
        ent = int(st.get("paper_entries_today_msk") or 0)
        ex = int(st.get("paper_exits_today_msk") or 0)
        sk = int(st.get("paper_skipped_today_msk") or 0)
    else:
        ent = ex = sk = 0

    by_city = _pnl_by_city_on_msk_date(port, d)
    best = worst = "—"
    if by_city:
        items = sorted(by_city.items(), key=lambda x: x[1], reverse=True)
        best = f"{items[0][0]} (${items[0][1]:+.2f})"
        worst = f"{items[-1][0]} (${items[-1][1]:+.2f})"

    lines = [
        f"📊 PAPER DAILY SUMMARY ({d.isoformat()} МСК)",
        f"cash: ${cash:.2f} | realized: ${realized:.2f} | unreal~: ${unreal:.2f}",
        f"open positions: {open_n}",
        f"today activity: entries +{ent} | exits {ex} | skipped {sk}",
        f"structures (cumulative): ladder_3={se.get('ladder_3', 0)} single={se.get('single_bucket', 0)}",
        f"best / worst city (closed that day): {best} / {worst}",
        f"logic: {fee_logic_version()} / {allocation_logic_version()} / {exit_logic_version()}",
    ]
    return "\n".join(lines)


def format_weekly_telegram_summary(week_end_msk: date | None = None) -> str:
    d = week_end_msk or datetime.now(ZoneInfo("Europe/Moscow")).date()
    port = load_portfolio()
    starting = float(port.get("starting_balance") or paper_start_balance())
    cash = float(port.get("current_cash") or 0)
    unreal = float(port.get("unrealized_pnl_estimate") or 0)
    realized = float(port.get("realized_pnl") or 0)
    open_pos = port.get("open_positions") or {}
    alloc = sum(float(p.get("total_allocated_usd") or 0) for p in open_pos.values())
    equity = cash + unreal + alloc
    roi = ((equity - starting) / starting) if starting else 0.0
    st = port.get("stats") or {}
    skipped = st.get("skipped_by_reason") or {}
    total_skipped = int(st.get("total_signals_skipped") or 0)
    taken = int(st.get("total_signals_taken") or 0)
    closed_n = len(port.get("closed_positions") or [])
    by_city: dict[str, float] = {}
    for c in port.get("closed_positions") or []:
        if not isinstance(c, dict):
            continue
        k = c.get("city_key") or "?"
        by_city[k] = by_city.get(k, 0.0) + float(c.get("realized_pnl") or 0)
    best = worst = "—"
    if by_city:
        items = sorted(by_city.items(), key=lambda x: x[1], reverse=True)
        best = f"{items[0][0]} (${items[0][1]:+.2f})"
        worst = f"{items[-1][0]} (${items[-1][1]:+.2f})"

    skip_txt = json.dumps(skipped, ensure_ascii=False) if skipped else "—"
    if len(skip_txt) > 220:
        skip_txt = skip_txt[:217] + "..."
    lines = [
        f"📅 PAPER WEEKLY SUMMARY (week end {d.isoformat()} МСК)",
        f"start ${starting:.2f} | equity~ ${equity:.2f} | cash ${cash:.2f}",
        f"realized: ${realized:.2f} | unreal~: ${unreal:.2f} | ROI: {roi*100:.2f}%",
        f"entries (total): {taken} | exits (closed pos): {closed_n} | skips (total): {total_skipped}",
        f"skip breakdown: {skip_txt}",
        f"open now: {len(open_pos)} | best city: {best} | worst: {worst}",
        f"logic: {fee_logic_version()} / {allocation_logic_version()} / {exit_logic_version()}",
    ]
    return "\n".join(lines)


def format_paper_status_message() -> str:
    port = load_portfolio()
    on = paper_trading_enabled()
    cash = float(port.get("current_cash") or 0)
    open_n = len(port.get("open_positions") or {})
    closed_n = len(port.get("closed_positions") or [])
    realized = float(port.get("realized_pnl") or 0)
    unreal = float(port.get("unrealized_pnl_estimate") or 0)
    st = port.get("stats") or {}
    taken = int(st.get("total_signals_taken") or 0)
    skipped = int(st.get("total_signals_skipped") or 0)
    lu = port.get("last_updated_utc") or "—"
    last = "—"
    pj = os.environ.get("PAPER_TRADE_JOURNAL_PATH") or ""
    try:
        if pj and os.path.isfile(pj) and os.path.getsize(pj) > 0:
            with open(pj, "rb") as f:
                f.seek(-min(4096, os.path.getsize(pj)), 2)
                tail = f.read().decode("utf-8", errors="replace").splitlines()[-1]
            rec = json.loads(tail)
            last = f"{rec.get('action_type')} @ {rec.get('timestamp_msk', rec.get('timestamp_utc'))}"
    except Exception:
        pass

    lines = [
        "📎 PAPER STATUS",
        f"enabled: {'yes' if on else 'no'}",
        f"cash: ${cash:.2f} | open: {open_n} | closed (total): {closed_n}",
        f"realized: ${realized:.2f} | unreal~: ${unreal:.2f}",
        f"signals taken: {taken} | skipped: {skipped}",
        f"last portfolio update: {lu}",
        f"last journal: {last}",
    ]
    return "\n".join(lines)
