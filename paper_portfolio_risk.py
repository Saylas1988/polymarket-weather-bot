"""
Портфельные лимиты открытого риска (v2): caps по событиям, городам, датам, drawdown pause.

Не меняет allocator/entry rules — только gating перед открытием позиции.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from paper_settings import (
    paper_max_city_exposure_pct,
    paper_max_open_events,
    paper_max_open_events_per_city,
    paper_max_same_date_exposure_pct,
    paper_max_total_open_exposure_pct,
    paper_start_balance,
    paper_unrealized_drawdown_hard_pct,
    paper_unrealized_drawdown_pause_pct,
)


def _norm_event_date(s: str | None) -> str:
    if not s:
        return ""
    t = str(s).strip()
    return t[:10] if len(t) >= 10 else t


def unrealized_drawdown_vs_bankroll(portfolio: dict[str, Any]) -> tuple[float, float]:
    """
    Если суммарный mark-to-market unrealized < 0, «просадка» = -unrealized в USD и как доля bankroll.
    Bankroll = starting_balance из портфеля (эталон), не текущий cash.
    """
    unreal = float(portfolio.get("unrealized_pnl_estimate") or 0.0)
    bankroll = float(portfolio.get("starting_balance") or 0.0) or paper_start_balance()
    if unreal >= 0:
        return 0.0, 0.0
    dd_usd = -unreal
    dd_pct = dd_usd / bankroll if bankroll > 0 else 0.0
    return round(dd_usd, 4), round(dd_pct, 6)


def determine_risk_mode(portfolio: dict[str, Any]) -> str:
    """normal | drawdown_pause | hard_reduction"""
    _, dd_pct = unrealized_drawdown_vs_bankroll(portfolio)
    hard = paper_unrealized_drawdown_hard_pct()
    pause = paper_unrealized_drawdown_pause_pct()
    if hard > 0 and dd_pct >= hard:
        return "hard_reduction"
    if pause > 0 and dd_pct >= pause:
        return "drawdown_pause"
    return "normal"


def aggregate_open_risk(portfolio: dict[str, Any]) -> dict[str, Any]:
    """Снимок открытых позиций: exposure USD, по городам и датам события."""
    open_pos = portfolio.get("open_positions") or {}
    bankroll = float(portfolio.get("starting_balance") or 0.0) or paper_start_balance()
    by_city: dict[str, dict[str, float | int]] = {}
    by_date: dict[str, float] = {}
    total_exp = 0.0
    n_events = 0

    for _slug, pos in open_pos.items():
        if not isinstance(pos, dict):
            continue
        n_events += 1
        exp = float(pos.get("total_allocated_usd") or 0.0)
        total_exp += exp
        ck = str(pos.get("city_key") or "?")
        bc = by_city.setdefault(ck, {"open_events": 0, "exposure_usd": 0.0})
        bc["open_events"] = int(bc["open_events"]) + 1
        bc["exposure_usd"] = float(bc["exposure_usd"]) + exp
        ed = pos.get("event_date")
        if isinstance(ed, str):
            dk = _norm_event_date(ed)
        elif hasattr(ed, "isoformat"):
            dk = ed.isoformat()[:10]
        else:
            dk = str(ed)[:10] if ed else "?"
        by_date[dk] = by_date.get(dk, 0.0) + exp

    dd_usd, dd_pct = unrealized_drawdown_vs_bankroll(portfolio)
    mode = determine_risk_mode(portfolio)

    return {
        "bankroll_reference_usd": round(bankroll, 4),
        "open_events_count": n_events,
        "total_open_exposure_usd": round(total_exp, 4),
        "total_open_exposure_pct": round((total_exp / bankroll) if bankroll > 0 else 0.0, 6),
        "by_city": {k: {"open_events": int(v["open_events"]), "exposure_usd": round(float(v["exposure_usd"]), 4)} for k, v in by_city.items()},
        "by_event_date": {k: round(v, 4) for k, v in sorted(by_date.items())},
        "unrealized_pnl_estimate": float(portfolio.get("unrealized_pnl_estimate") or 0.0),
        "unrealized_drawdown_usd": dd_usd,
        "unrealized_drawdown_pct_of_bankroll": dd_pct,
        "risk_mode": mode,
        "updated_at_utc": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
    }


def refresh_portfolio_risk_state(portfolio: dict[str, Any]) -> dict[str, Any]:
    """Пишет portfolio['risk_state'] для /paper и отчётов."""
    snap = aggregate_open_risk(portfolio)
    portfolio["risk_state"] = snap
    return snap


def portfolio_risk_allows_new_entry(
    portfolio: dict[str, Any],
    *,
    city_key: str,
    event_date: str | None,
    proposed_budget_usd: float,
) -> tuple[bool, str, dict[str, Any]]:
    """
    Возвращает (allowed, reason_code, detail).

    reason_code при отказе: portfolio_risk_hard_reduction | portfolio_risk_drawdown_pause |
    portfolio_risk_max_open_events | portfolio_risk_max_city_events |
    portfolio_risk_max_total_open_exposure | portfolio_risk_max_city_exposure |
    portfolio_risk_max_same_date_exposure | ok
    """
    detail: dict[str, Any] = {}
    ck = (city_key or "").strip() or "?"
    ed = _norm_event_date(event_date)
    prop = max(0.0, float(proposed_budget_usd))

    bankroll = float(portfolio.get("starting_balance") or 0.0) or paper_start_balance()
    snap = aggregate_open_risk(portfolio)
    detail["snapshot"] = snap

    mode = snap["risk_mode"]
    if mode == "hard_reduction":
        return False, "portfolio_risk_hard_reduction", detail
    if mode == "drawdown_pause":
        return False, "portfolio_risk_drawdown_pause", detail

    max_ev = paper_max_open_events()
    if max_ev > 0 and snap["open_events_count"] >= max_ev:
        detail["limit"] = max_ev
        return False, "portfolio_risk_max_open_events", detail

    max_pc = paper_max_open_events_per_city()
    if max_pc > 0:
        cinfo = snap["by_city"].get(ck) or {"open_events": 0}
        if int(cinfo.get("open_events") or 0) >= max_pc:
            detail["limit"] = max_pc
            detail["city_key"] = ck
            return False, "portfolio_risk_max_city_events", detail

    total_cap = paper_max_total_open_exposure_pct()
    if total_cap > 0:
        total_after = snap["total_open_exposure_usd"] + prop
        if total_after > total_cap * bankroll + 1e-6:
            detail["cap_pct"] = total_cap
            detail["total_after_usd"] = round(total_after, 4)
            detail["cap_usd"] = round(total_cap * bankroll, 4)
            return False, "portfolio_risk_max_total_open_exposure", detail

    city_cap = paper_max_city_exposure_pct()
    if city_cap > 0:
        city_exp = float((snap["by_city"].get(ck) or {}).get("exposure_usd") or 0.0)
        if city_exp + prop > city_cap * bankroll + 1e-6:
            detail["cap_pct"] = city_cap
            detail["city_key"] = ck
            detail["city_after_usd"] = round(city_exp + prop, 4)
            return False, "portfolio_risk_max_city_exposure", detail

    sd_cap = paper_max_same_date_exposure_pct()
    if sd_cap > 0 and ed:
        d_exp = float(snap["by_event_date"].get(ed) or 0.0)
        if d_exp + prop > sd_cap * bankroll + 1e-6:
            detail["cap_pct"] = sd_cap
            detail["event_date"] = ed
            detail["date_after_usd"] = round(d_exp + prop, 4)
            return False, "portfolio_risk_max_same_date_exposure", detail

    return True, "ok", detail
