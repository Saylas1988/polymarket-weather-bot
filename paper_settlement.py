"""
Принудительное закрытие paper-позиций после market verification (resolution YES/NO).

Не меняет правила входа/выхода до верификации; только завершает жизненный цикл, когда известен
winning bucket, чтобы verified-событие не оставалось в open_positions.
"""

from __future__ import annotations

import datetime as dt
import logging
import re
from typing import Any

from zoneinfo import ZoneInfo

import main as m
from paper_fee_logic import fee_usd_exit_for_mode, round_fee as fee_round
from paper_portfolio import load_portfolio, save_portfolio
from paper_portfolio_risk import refresh_portfolio_risk_state
from paper_settings import paper_exit_mode, paper_trade_journal_path, paper_trading_enabled
from paper_trade_log import append_paper_trade_record

log = logging.getLogger("rainmaker")


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _norm_label(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _winning_condition_id(event: dict[str, Any], winning_label: str) -> str | None:
    wn = _norm_label(winning_label)
    for mk in event.get("markets") or []:
        if not isinstance(mk, dict):
            continue
        q = (mk.get("question") or mk.get("title") or "").strip()
        if _norm_label(q) == wn:
            cid = mk.get("conditionId") or mk.get("condition_id")
            if cid:
                return str(cid).strip()
    return None


def leg_is_winning(leg: dict[str, Any], *, winning_label: str, winning_condition_id: str | None) -> bool:
    """Сопоставление ноги с победившим рынком: приоритет condition_id, иначе текст вопроса."""
    lc = (leg.get("condition_id") or "").strip()
    if winning_condition_id and lc and lc == winning_condition_id:
        return True
    lq = _norm_label(str(leg.get("question") or ""))
    bq = _norm_label(str(leg.get("bucket_label") or ""))
    wn = _norm_label(winning_label)
    if wn and (wn == lq or wn == bq):
        return True
    if wn and lq and (wn in lq or lq in wn):
        return True
    return False


def _journal(path: str, action: str, payload: dict[str, Any]) -> None:
    u = _utc_now().replace(microsecond=0)
    msk = dt.datetime.now(ZoneInfo("Europe/Moscow")).replace(microsecond=0)
    rec = {
        "timestamp_utc": u.isoformat(),
        "timestamp_msk": msk.isoformat(),
        "action_type": action,
        **payload,
    }
    append_paper_trade_record(rec, path=path)


def run_paper_settlement_pass(*, now: dt.datetime | None = None) -> int:
    """
    Для каждой открытой позиции с заполненным market_verification:
    принудительно закрывает оставшиеся ноги по резолву ($1 / $0), считает слои PnL,
    переносит позицию в closed_positions.

    Возвращает число позиций, обработанных за вызов.
    """
    if not paper_trading_enabled():
        return 0

    now = now or _utc_now()
    path = paper_trade_journal_path()
    portfolio = load_portfolio()
    open_map = portfolio.get("open_positions") or {}
    if not open_map:
        return 0

    closed = list(portfolio.get("closed_positions") or [])
    realized = float(portfolio.get("realized_pnl") or 0.0)
    cash = float(portfolio.get("current_cash") or 0.0)
    stats = portfolio.setdefault("stats", {})

    em = paper_exit_mode()
    exit_mode = em if em in ("maker_like", "taker_like") else "maker_like"

    done = 0
    to_del: list[str] = []

    for slug, pos in list(open_map.items()):
        if not isinstance(pos, dict):
            continue
        ver = pos.get("market_verification")
        if not isinstance(ver, dict):
            continue
        if pos.get("settlement_applied"):
            continue
        win_label = ver.get("winning_bucket_label")
        if not win_label:
            continue

        try:
            event = m._gamma_get_event_by_slug(slug)
        except Exception as e:
            log.warning("settlement: gamma %s: %s", slug, e)
            continue

        win_cid = _winning_condition_id(event, str(win_label))

        legs = list(pos.get("legs") or [])
        trade_pnl_pre = round(
            sum(float(lg.get("virtual_realized_pnl") or 0) for lg in legs if lg.get("status") == "closed_virtual"),
            4,
        )

        settlement_lines: list[dict[str, Any]] = []
        settlement_pnl = 0.0

        for leg in legs:
            if leg.get("status") == "closed_virtual":
                continue

            ctr = float(leg.get("estimated_contracts") or 0)
            leg_cost = float(leg.get("allocated_usd") or 0)
            fee_in_leg = float(leg.get("entry_fee_allocated") or 0)
            is_win = leg_is_winning(leg, winning_label=str(win_label), winning_condition_id=win_cid)

            proceeds = ctr * 1.0 if is_win else 0.0
            px_fee = 1.0 if is_win else 0.0
            fee_out = fee_round(fee_usd_exit_for_mode(proceeds, px_fee, mode=exit_mode))
            leg_st_pnl = proceeds - leg_cost - fee_in_leg - fee_out
            settlement_pnl += leg_st_pnl
            cash += proceeds - fee_out
            realized += leg_st_pnl

            leg["status"] = "closed_settlement"
            leg["closed_at_utc"] = now.replace(microsecond=0).isoformat()
            leg["settlement_winner"] = bool(is_win)
            leg["settlement_proceeds_usd"] = round(proceeds, 4)
            leg["settlement_exit_fee_usd"] = fee_out
            leg["settlement_leg_pnl"] = round(leg_st_pnl, 4)

            settlement_lines.append(
                {
                    "leg_key": leg.get("leg_key"),
                    "settlement_winner": is_win,
                    "settlement_leg_pnl": round(leg_st_pnl, 4),
                }
            )

        settlement_pnl = round(settlement_pnl, 4)
        final_pnl = round(trade_pnl_pre + settlement_pnl, 4)

        settled_kind = "settled_win" if final_pnl >= 0 else "settled_loss"
        pos["lifecycle_status"] = settled_kind
        pos["status"] = "closed"
        pos["settlement_applied"] = True
        pos["settlement_applied_at_utc"] = now.replace(microsecond=0).isoformat()
        pos["trade_pnl_pre_settlement"] = trade_pnl_pre
        pos["settlement_pnl"] = settlement_pnl
        pos["final_pnl"] = final_pnl
        pos["exit_kind"] = "market_settlement"
        pos["closed_at_utc"] = now.replace(microsecond=0).isoformat()
        pos["realized_pnl"] = final_pnl

        closed.append(pos)
        to_del.append(slug)
        done += 1

        stats["settlements_completed"] = int(stats.get("settlements_completed") or 0) + 1

        _journal(
            path,
            "position_settlement",
            {
                "event_slug": slug,
                "paper_trade_id": pos.get("paper_trade_id"),
                "lifecycle_status": settled_kind,
                "trade_pnl_pre_settlement": trade_pnl_pre,
                "settlement_pnl": settlement_pnl,
                "final_pnl": final_pnl,
                "cash_after": round(cash, 4),
                "realized_pnl_total_after": round(realized, 4),
                "winning_bucket_label": win_label,
                "verified_temperature_c": ver.get("verified_temperature_c"),
                "settlement_legs": settlement_lines,
            },
        )

    for slug in to_del:
        open_map.pop(slug, None)

    portfolio["open_positions"] = open_map
    portfolio["closed_positions"] = closed[-500:]
    portfolio["realized_pnl"] = round(realized, 4)
    portfolio["current_cash"] = round(cash, 4)
    portfolio["stats"]["closed_count"] = len(closed)
    portfolio["last_updated_utc"] = _utc_now().replace(microsecond=0).isoformat()

    if done:
        refresh_portfolio_risk_state(portfolio)
        save_portfolio(portfolio)
        log.info("paper settlement: закрыто позиций: %s", done)

    return done
