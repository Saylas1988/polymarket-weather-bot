"""
Ежедневные и недельные отчёты paper trading (файлы JSON + краткий текст).
"""

from __future__ import annotations

import json
import os
from datetime import date
from typing import Any

from paper_portfolio import load_portfolio
from paper_portfolio_risk import refresh_portfolio_risk_state
from paper_settings import (
    allocation_logic_version,
    exit_logic_version,
    fee_logic_version,
    paper_portfolio_path,
    paper_reports_dir,
    paper_start_balance,
    week1_paper_test_freeze_snapshot,
)
from paper_telegram_messages import format_daily_telegram_summary, format_weekly_telegram_summary


def _ensure_dir(d: str) -> None:
    os.makedirs(d, exist_ok=True)


def _skipped_reasons_summary(skipped: dict[str, Any]) -> dict[str, Any]:
    """Человекочитаемые алиасы для отчётов (дубли события и т.д.)."""
    out = dict(skipped) if isinstance(skipped, dict) else {}
    if int(skipped.get("already_open") or 0) > 0:
        out["note_duplicate_open"] = (
            "already_open считается «дубликатом» открытой позиции по тому же event_slug (one event = one position)."
        )
    pr_notes = {
        "portfolio_risk_hard_reduction": "лимит просадки unrealized (hard): новые входы запрещены",
        "portfolio_risk_drawdown_pause": "лимит просадки unrealized (pause): новые входы запрещены",
        "portfolio_risk_max_open_events": "достигнут PAPER_MAX_OPEN_EVENTS",
        "portfolio_risk_max_city_events": "достигнут PAPER_MAX_OPEN_EVENTS_PER_CITY",
        "portfolio_risk_max_total_open_exposure": "достигнут PAPER_MAX_TOTAL_OPEN_EXPOSURE_PCT",
        "portfolio_risk_max_city_exposure": "достигнут PAPER_MAX_CITY_EXPOSURE_PCT",
        "portfolio_risk_max_same_date_exposure": "достигнут PAPER_MAX_SAME_DATE_EXPOSURE_PCT",
    }
    for k, note in pr_notes.items():
        if int(skipped.get(k) or 0) > 0:
            out[f"note_{k}"] = note
    return out


def write_daily_report_file(for_day_msk: date | None = None) -> str:
    """Пишет paper_reports/daily_YYYY-MM-DD.json, возвращает путь."""
    d = for_day_msk or date.today()
    pdir = paper_reports_dir()
    _ensure_dir(pdir)
    path = os.path.join(pdir, f"daily_{d.isoformat()}.json")

    port = load_portfolio()
    refresh_portfolio_risk_state(port)
    open_n = len(port.get("open_positions") or {})
    closed = port.get("closed_positions") or []
    realized = float(port.get("realized_pnl") or 0)
    unreal = float(port.get("unrealized_pnl_estimate") or 0)
    cash = float(port.get("current_cash") or 0)
    starting = float(port.get("starting_balance") or paper_start_balance())
    equity = cash + unreal + sum(
        float(p.get("total_allocated_usd") or 0) for p in (port.get("open_positions") or {}).values()
    )

    stats = port.get("stats") or {}
    skipped = stats.get("skipped_by_reason") or {}

    payload = {
        "report_date_msk": d.isoformat(),
        "version_tags": {
            "allocator_logic_version": allocation_logic_version(),
            "fee_logic_version": fee_logic_version(),
            "exit_logic_version": exit_logic_version(),
        },
        "week1_paper_test_freeze": week1_paper_test_freeze_snapshot(),
        "current_cash": cash,
        "starting_balance": starting,
        "realized_pnl": realized,
        "unrealized_pnl_estimate": unreal,
        "equity_estimate": round(equity, 4),
        "opened_positions_count": open_n,
        "closed_positions_total": len(closed),
        "stats": stats,
        "structure_entries": stats.get("structure_entries") or {},
        "allocator": {
            "forced_single_from_ladder_count": stats.get("allocator_forced_single_from_ladder"),
            "partial_cuts_count": stats.get("allocator_partial_cuts"),
        },
        "skipped_signals_summary": _skipped_reasons_summary(skipped),
        "exit_reasons_summary": stats.get("exit_reasons") or {},
        "risk_state": port.get("risk_state"),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path


def write_weekly_report_file(week_end_msk: date | None = None) -> str:
    d = week_end_msk or date.today()
    pdir = paper_reports_dir()
    _ensure_dir(pdir)
    path = os.path.join(pdir, f"weekly_{d.isoformat()}.json")

    port = load_portfolio()
    refresh_portfolio_risk_state(port)
    starting = float(port.get("starting_balance") or paper_start_balance())
    cash = float(port.get("current_cash") or 0)
    unreal = float(port.get("unrealized_pnl_estimate") or 0)
    realized = float(port.get("realized_pnl") or 0)
    open_pos = port.get("open_positions") or {}
    alloc = sum(float(p.get("total_allocated_usd") or 0) for p in open_pos.values())
    equity = cash + unreal + alloc
    roi = ((equity - starting) / starting) if starting else 0.0

    by_city: dict[str, float] = {}
    for c in port.get("closed_positions") or []:
        if not isinstance(c, dict):
            continue
        k = c.get("city_key") or "?"
        by_city[k] = by_city.get(k, 0.0) + float(c.get("realized_pnl") or 0)

    stats = port.get("stats") or {}
    skipped = stats.get("skipped_by_reason") or {}

    payload = {
        "week_end_msk": d.isoformat(),
        "version_tags": {
            "allocator_logic_version": allocation_logic_version(),
            "fee_logic_version": fee_logic_version(),
            "exit_logic_version": exit_logic_version(),
        },
        "week1_paper_test_freeze": week1_paper_test_freeze_snapshot(),
        "starting_balance": starting,
        "current_cash": cash,
        "unrealized_pnl_estimate": unreal,
        "realized_pnl": realized,
        "equity_estimate": round(equity, 4),
        "roi_estimate": round(roi, 6),
        "open_positions_at_end": list(open_pos.keys()),
        "pnl_by_city_closed": by_city,
        "stats": stats,
        "structure_entries": stats.get("structure_entries") or {},
        "allocator": {
            "forced_single_from_ladder_count": stats.get("allocator_forced_single_from_ladder"),
            "partial_cuts_count": stats.get("allocator_partial_cuts"),
        },
        "skipped_signals_summary": _skipped_reasons_summary(skipped),
        "exit_reasons_summary": stats.get("exit_reasons") or {},
        "risk_state": port.get("risk_state"),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path


# format_daily_telegram_summary / format_weekly_telegram_summary — см. paper_telegram_messages
