"""
Этап 3: paper trading engine — ранжирование, открытие, mark-to-mid, виртуальный выход по target.
"""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

from zoneinfo import ZoneInfo

from paper_allocation import generate_bucket_allocation
from paper_fee_logic import fee_usd_exit_for_mode, round_fee as fee_round
from paper_portfolio import load_portfolio, save_portfolio
from paper_settings import (
    allocation_logic_version,
    exit_logic_version,
    fee_logic_version,
    paper_allow_reentry_same_event,
    paper_close_mode,
    paper_enable_virtual_sell_plan,
    paper_exit_mode,
    paper_max_new_positions_per_cycle,
    paper_max_risk_per_trade_pct,
    paper_min_allocation_usd,
    paper_signal_ttl_minutes,
    paper_start_balance,
    paper_trade_journal_path,
    paper_trading_enabled,
    signal_logic_version,
)
from paper_trade_log import append_paper_trade_record
from station_config import city_config_by_display_name


def _roll_paper_msk_activity(portfolio: dict[str, Any]) -> None:
    """Сброс/архив суточных счётчиков paper по календарю МСК."""
    st = portfolio.setdefault("stats", {})
    today_msk = _msk_now().date().isoformat()
    cur = st.get("paper_activity_date_msk")
    if cur is None:
        st["paper_activity_date_msk"] = today_msk
        st.setdefault("paper_entries_today_msk", 0)
        st.setdefault("paper_exits_today_msk", 0)
        st.setdefault("paper_skipped_today_msk", 0)
        return
    if cur == today_msk:
        return
    st["paper_prev_msk_summary"] = {
        "date": cur,
        "entries": int(st.get("paper_entries_today_msk") or 0),
        "exits": int(st.get("paper_exits_today_msk") or 0),
        "skipped": int(st.get("paper_skipped_today_msk") or 0),
    }
    st["paper_activity_date_msk"] = today_msk
    st["paper_entries_today_msk"] = 0
    st["paper_exits_today_msk"] = 0
    st["paper_skipped_today_msk"] = 0


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _msk_now() -> dt.datetime:
    return dt.datetime.now(ZoneInfo("Europe/Moscow"))


def _journal(path: str, action: str, payload: dict[str, Any]) -> None:
    u = _utc_now().replace(microsecond=0)
    m = _msk_now().replace(microsecond=0)
    rec = {
        "timestamp_utc": u.isoformat(),
        "timestamp_msk": m.isoformat(),
        "action_type": action,
        **payload,
    }
    append_paper_trade_record(rec, path=path)


def _find_market_for_leg(markets: list[Any], leg: dict[str, Any]) -> dict[str, Any] | None:
    """Сопоставление ноги с рынком Gamma: condition_id > gamma_market_id > question."""
    cid = (leg.get("condition_id") or "").strip()
    mid = (leg.get("gamma_market_id") or "").strip()
    q = (leg.get("question") or "").strip()
    for mk in markets:
        if not isinstance(mk, dict):
            continue
        if cid:
            mc = str(mk.get("conditionId") or mk.get("condition_id") or "").strip()
            if mc and mc == cid:
                return mk
        if mid and str(mk.get("id") or "").strip() == mid:
            return mk
    for mk in markets:
        if not isinstance(mk, dict):
            continue
        qq = (mk.get("question") or mk.get("title") or "").strip()
        if q and qq == q.strip():
            return mk
    return None


def _ranking_key(item: tuple[str, dict[str, Any], dt.datetime]) -> tuple:
    """Выше gap_entry, уже спред, выше объём, меньше depth, выше gap_analytical."""
    _slug, res, _t = item
    ge = res.get("gap_entry")
    ge_k = -float(ge) if isinstance(ge, (int, float)) else 999.0
    sp = res.get("spread_main")
    sp_k = float(sp) if isinstance(sp, (int, float)) else 999.0
    vm = res.get("volume_main")
    vm_k = -float(vm) if isinstance(vm, (int, float)) else 0.0
    dep = int(res.get("depth") or 9)
    ga = res.get("gap_analytical")
    ga_k = -float(ga) if isinstance(ga, (int, float)) else 0.0
    return (ge_k, sp_k, vm_k, dep, ga_k)


def _eligible(
    res: dict[str, Any],
    evaluated_at: dt.datetime,
    now: dt.datetime,
    open_slugs: set[str],
    closed_slugs: set[str],
) -> tuple[bool, str]:
    if not res.get("signal"):
        return False, "not_signal"
    if res.get("structure_type") == "no_trade":
        return False, "structure_no_trade"
    if not res.get("ok_liquidity"):
        return False, "liquidity"
    if res.get("city_mode") != "paper_only":
        return False, "city_mode"
    slug = res.get("event_slug") or ""
    if slug in open_slugs:
        return False, "already_open"
    if not paper_allow_reentry_same_event() and slug in closed_slugs:
        return False, "already_closed_no_reentry"
    ttl_m = paper_signal_ttl_minutes()
    age_s = (now - evaluated_at).total_seconds()
    if age_s > ttl_m * 60 + 5:  # небольшой запас на часы
        return False, "ttl_expired"
    return True, "ok"


def run_paper_phase(
    batch: list[tuple[str, dict[str, Any], dt.datetime]],
    *,
    now: dt.datetime | None = None,
    ecmwf_bulletin_recheck: bool = False,
) -> None:
    """
    batch: список (event_slug, result evaluate_signal_for_event, evaluated_at_utc-aware).
    """
    if not paper_trading_enabled():
        return

    now = now or _utc_now()
    path = paper_trade_journal_path()
    init_paper_if_missing()
    portfolio = load_portfolio()
    portfolio.setdefault("stats", {})
    _roll_paper_msk_activity(portfolio)

    import main as m

    # 1) mark / exit по открытым
    _update_and_exit_open_positions(m, portfolio, now, path, ecmwf_bulletin_recheck=ecmwf_bulletin_recheck)

    # 2) кандидаты на вход
    open_slugs = set(portfolio.get("open_positions") or {})
    closed_slugs = {c.get("event_slug") for c in (portfolio.get("closed_positions") or []) if isinstance(c, dict)}

    candidates: list[tuple[str, dict[str, Any], dt.datetime]] = []
    for event_slug, res, ev_at in batch:
        if res.get("skip"):
            continue
        if res.get("signal"):
            portfolio["stats"]["total_signals_seen"] = int(portfolio["stats"].get("total_signals_seen") or 0) + 1
        else:
            continue

        ok, reason = _eligible(res, ev_at, now, open_slugs, closed_slugs)
        if not ok:
            portfolio["stats"]["total_signals_skipped"] = int(portfolio["stats"].get("total_signals_skipped") or 0) + 1
            portfolio["stats"]["paper_skipped_today_msk"] = int(portfolio["stats"].get("paper_skipped_today_msk") or 0) + 1
            sr = portfolio["stats"].setdefault("skipped_by_reason", {})
            sr[reason] = int(sr.get(reason) or 0) + 1
            _journal(
                path,
                "open_skipped",
                {
                    "event_slug": event_slug,
                    "city_key": res.get("city_key"),
                    "station_code": res.get("station_code"),
                    "source_type": res.get("source_type"),
                    "structure_type": res.get("structure_type"),
                    "reason_if_skipped": reason,
                },
            )
            continue
        candidates.append((event_slug, res, ev_at))

    candidates.sort(key=_ranking_key)
    max_n = paper_max_new_positions_per_cycle()
    cash = float(portfolio.get("current_cash") or 0)

    opened = 0
    for event_slug, res, ev_at in candidates:
        if opened >= max_n:
            portfolio["stats"]["total_signals_skipped"] = int(portfolio["stats"].get("total_signals_skipped") or 0) + 1
            portfolio["stats"]["paper_skipped_today_msk"] = int(portfolio["stats"].get("paper_skipped_today_msk") or 0) + 1
            sr = portfolio["stats"].setdefault("skipped_by_reason", {})
            sr["cycle_limit"] = int(sr.get("cycle_limit") or 0) + 1
            _journal(
                path,
                "open_skipped",
                {
                    "event_slug": event_slug,
                    "reason_if_skipped": "cycle_limit",
                    "ranking_note": f"max_new_per_cycle={max_n}",
                },
            )
            continue

        stake = int(res.get("stake") or 0)
        risk_cap = cash * paper_max_risk_per_trade_pct()
        budget = float(min(stake, risk_cap, cash))
        if budget < paper_min_allocation_usd():
            portfolio["stats"]["total_signals_skipped"] = int(portfolio["stats"].get("total_signals_skipped") or 0) + 1
            portfolio["stats"]["paper_skipped_today_msk"] = int(portfolio["stats"].get("paper_skipped_today_msk") or 0) + 1
            sr = portfolio["stats"].setdefault("skipped_by_reason", {})
            sr["insufficient_cash_or_min"] = int(sr.get("insufficient_cash_or_min") or 0) + 1
            _journal(
                path,
                "open_skipped",
                {"event_slug": event_slug, "reason_if_skipped": "insufficient_cash_or_min", "cash_before": cash},
            )
            continue

        st = str(res.get("structure_type") or "no_trade")
        depth = int(res.get("depth") or 1)
        alloc = generate_bucket_allocation(
            total_budget_usd=budget,
            structure_type=st,
            res=res,
            depth=depth,
        )
        total_alloc = float(alloc.get("allocation_total_usd") or 0)
        fee_in = float(alloc.get("fee_estimate_entry") or 0)
        need = total_alloc + fee_in
        if need > cash + 1e-6:
            portfolio["stats"]["total_signals_skipped"] = int(portfolio["stats"].get("total_signals_skipped") or 0) + 1
            portfolio["stats"]["paper_skipped_today_msk"] = int(portfolio["stats"].get("paper_skipped_today_msk") or 0) + 1
            sr = portfolio["stats"].setdefault("skipped_by_reason", {})
            sr["insufficient_cash_or_min"] = int(sr.get("insufficient_cash_or_min") or 0) + 1
            continue

        pos = _build_position_dict(event_slug, res, alloc, now, m)
        portfolio["open_positions"][event_slug] = pos
        portfolio["current_cash"] = round(cash - need, 4)
        portfolio["stats"]["total_signals_taken"] = int(portfolio["stats"].get("total_signals_taken") or 0) + 1
        cash = float(portfolio["current_cash"])
        opened += 1
        open_slugs.add(event_slug)

        eff = str(alloc.get("structure_type_effective") or st)
        se = portfolio["stats"].setdefault("structure_entries", {})
        se[eff] = int(se.get(eff) or 0) + 1
        if st == "ladder_3" and eff == "single_bucket":
            portfolio["stats"]["allocator_forced_single_from_ladder"] = int(
                portfolio["stats"].get("allocator_forced_single_from_ladder") or 0
            ) + 1
        ncuts = alloc.get("neighbor_cuts") or []
        if isinstance(ncuts, list) and len(ncuts) > 0:
            portfolio["stats"]["allocator_partial_cuts"] = int(portfolio["stats"].get("allocator_partial_cuts") or 0) + 1

        _journal(
            path,
            "position_opened",
            {
                "event_slug": event_slug,
                "city_key": res.get("city_key"),
                "station_code": res.get("station_code"),
                "source_type": res.get("source_type"),
                "structure_type_requested": st,
                "structure_type_effective": eff,
                "allocator_notes": alloc.get("allocator_notes"),
                "neighbor_cuts": alloc.get("neighbor_cuts"),
                "requested_stake": stake,
                "final_allocated_stake": total_alloc,
                "cash_before": cash + need,
                "cash_after": cash,
                "target_summary": alloc.get("target_sell_prices"),
                "fees_summary": {"entry": fee_in, "exit_assumed": alloc.get("fee_estimate_exit_assumed")},
                "ranking_priority": "gap_entry_then_spread_then_volume_then_depth",
                "ecmwf_recheck_context": ecmwf_bulletin_recheck,
                "paper_close_mode": paper_close_mode(),
                "exit_logic_version": exit_logic_version(),
                "ensemble_run_trend": {
                    "previous_p_main": res.get("previous_p_main"),
                    "current_p_main": res.get("current_p_main"),
                    "delta_model": res.get("delta_model"),
                    "trend_label": res.get("trend_label"),
                },
            },
        )
        portfolio["stats"]["paper_entries_today_msk"] = int(portfolio["stats"].get("paper_entries_today_msk") or 0) + 1
        try:
            from paper_telegram_messages import format_paper_entry_message

            m.send_paper_telegram_safe(
                format_paper_entry_message(
                    display_name_en=str(pos.get("display_name_en") or "?"),
                    event_slug=event_slug,
                    station_code=pos.get("station_code"),
                    depth=(int(pos["depth"]) if pos.get("depth") is not None else None),
                    structure_type_requested=st,
                    structure_type_effective=eff,
                    total_allocated_usd=float(total_alloc),
                    cash_before=float(cash + need),
                    cash_after=float(cash),
                    allocator_notes=alloc.get("allocator_notes") if isinstance(alloc.get("allocator_notes"), list) else None,
                    neighbor_cuts=alloc.get("neighbor_cuts") if isinstance(alloc.get("neighbor_cuts"), list) else None,
                    target_summary=alloc.get("target_sell_prices") if isinstance(alloc.get("target_sell_prices"), dict) else None,
                ),
                event_slug=event_slug,
            )
        except Exception:
            pass

    portfolio["unrealized_pnl_estimate"] = _compute_unrealized(portfolio)
    portfolio["last_updated_utc"] = _utc_now().replace(microsecond=0).isoformat()
    save_portfolio(portfolio)


def _build_position_dict(
    event_slug: str,
    res: dict[str, Any],
    alloc: dict[str, Any],
    now: dt.datetime,
    m: Any,
) -> dict[str, Any]:
    cfg = city_config_by_display_name(res["city"])
    tid = str(uuid.uuid4())
    targets = alloc.get("target_sell_prices") or {}
    legs: list[dict[str, Any]] = []

    tot_usd = float(alloc.get("allocation_total_usd") or 0) or 1.0
    fee_in_total = float(alloc.get("fee_estimate_entry") or 0)
    fee_out_total = float(alloc.get("fee_estimate_exit_assumed") or 0)

    def add_leg(key: str, bucket: dict[str, Any] | None, usd: float) -> None:
        if bucket is None or usd <= 0:
            return
        ep = float(alloc.get("max_entry_price_per_leg", {}).get(key) or bucket.get("best_ask") or bucket.get("yes") or 0)
        contracts = (usd / ep) if ep > 0 else 0.0
        tgt = float(targets.get(key) or 0)
        share = usd / tot_usd
        leg_fee_in = fee_in_total * share
        tgt_fee = fee_out_total * share
        legs.append(
            {
                "leg_key": key,
                "bucket_label": m._format_bucket_label(bucket, res.get("unit") or "C"),
                "question": (bucket.get("question") or bucket.get("title") or "")[:500],
                "gamma_market_id": (bucket.get("gamma_market_id") or "")[:120],
                "condition_id": (bucket.get("condition_id") or "")[:160],
                "side": "YES",
                "allocated_usd": round(usd, 4),
                "entry_fee_allocated": round(leg_fee_in, 4),
                "entry_price_assumed": ep,
                "estimated_contracts": round(contracts, 6),
                "analytical_price_at_entry": float(bucket.get("yes") or 0),
                "best_bid_at_entry": bucket.get("best_bid"),
                "best_ask_at_entry": bucket.get("best_ask"),
                "spread_at_entry": res.get("spread_main") if key == "main" else None,
                "volume_at_entry": bucket.get("volume"),
                "target_sell_price": round(tgt, 4),
                "target_sell_type": "virtual_limit",
                "target_fee_estimate": round(tgt_fee, 4),
                "current_mark_price": None,
                "current_unrealized_pnl_estimate": None,
                "status": "open",
            }
        )

    st = str(res.get("structure_type") or "")
    if st == "single_bucket":
        add_leg("main", res.get("main"), float(alloc.get("main_bucket_usd") or 0))
    else:
        add_leg("lower", res.get("low"), float(alloc.get("lower_bucket_usd") or 0))
        add_leg("main", res.get("main"), float(alloc.get("main_bucket_usd") or 0))
        add_leg("upper", res.get("high"), float(alloc.get("upper_bucket_usd") or 0))

    u = now.replace(microsecond=0)
    return {
        "paper_trade_id": tid,
        "status": "open",
        "opened_at_utc": u.isoformat(),
        "opened_at_msk": _msk_now().replace(microsecond=0).isoformat(),
        "city_key": cfg.city_key,
        "display_name_en": cfg.display_name_en,
        "display_name_ru": cfg.display_name_ru,
        "event_slug": event_slug,
        "event_url": f"https://polymarket.com/event/{event_slug}",
        "event_date": res.get("date"),
        "depth": res.get("depth"),
        "source_type": cfg.source_type,
        "station_code": cfg.station_code,
        "station_name": cfg.station_name,
        "structure_type": st,
        "signal_logic_version": signal_logic_version(),
        "allocation_logic_version": allocation_logic_version(),
        "fee_logic_version": fee_logic_version(),
        "exit_logic_version": exit_logic_version(),
        "paper_close_mode": paper_close_mode(),
        "total_allocated_usd": float(alloc.get("allocation_total_usd") or 0),
        "total_entry_fee_estimated": float(alloc.get("fee_estimate_entry") or 0),
        "target_exit_mode": paper_exit_mode(),
        "bulletins_seen_since_open": 0,
        "resolution_url": cfg.resolution_url,
        "resolution_context": res.get("resolution_context"),
        "legs": legs,
        "p_main_at_entry": res.get("p_main"),
        "gap_analytical_at_entry": res.get("gap_analytical"),
        "gap_entry_at_entry": res.get("gap_entry"),
        "last_mark_update_utc": None,
        "last_ecmwf_recheck_utc": None,
    }


def _compute_unrealized(portfolio: dict[str, Any]) -> float:
    s = 0.0
    for pos in (portfolio.get("open_positions") or {}).values():
        if not isinstance(pos, dict):
            continue
        for leg in pos.get("legs") or []:
            if leg.get("status") == "closed_virtual":
                continue
            u = leg.get("current_unrealized_pnl_estimate")
            if isinstance(u, (int, float)):
                s += float(u)
    return round(s, 4)


def _update_and_exit_open_positions(
    m: Any,
    portfolio: dict[str, Any],
    now: dt.datetime,
    journal_path: str,
    *,
    ecmwf_bulletin_recheck: bool,
) -> None:
    open_map = portfolio.get("open_positions") or {}
    if not open_map:
        return

    closed = list(portfolio.get("closed_positions") or [])
    realized = float(portfolio.get("realized_pnl") or 0)
    cash = float(portfolio.get("current_cash") or 0)
    stats = portfolio.setdefault("stats", {})
    ex = stats.setdefault("exit_reasons", {})

    to_del: list[str] = []
    em = paper_exit_mode()
    exit_mode = em if em in ("maker_like", "taker_like") else "maker_like"
    close_mode = paper_close_mode()

    for slug, pos in list(open_map.items()):
        if not isinstance(pos, dict):
            continue
        try:
            event = m._gamma_get_event_by_slug(slug)
        except Exception as e:
            _journal(
                journal_path,
                "warning",
                {"event_slug": slug, "reason_if_skipped": f"gamma_fetch_failed:{e}"},
            )
            continue

        markets = event.get("markets") or []
        legs = pos.get("legs") or []
        all_hit = True

        for leg in legs:
            if leg.get("status") == "closed_virtual":
                continue

            mk = _find_market_for_leg(markets, leg)
            mark = None
            bb = None
            if mk is not None:
                liq = m._yes_price_and_liquidity(mk)
                mark = float(liq["yes"])
                bb = liq.get("best_bid")
            if mark is None:
                all_hit = False
                continue

            leg["current_mark_price"] = mark
            ep = float(leg.get("entry_price_assumed") or 0)
            ctr = float(leg.get("estimated_contracts") or 0)
            leg["current_unrealized_pnl_estimate"] = round((mark - ep) * ctr, 4)

            tgt = float(leg.get("target_sell_price") or 0)
            filled = False
            if exit_mode == "taker_like":
                filled = mark >= tgt - 1e-9
            else:
                filled = bb is not None and float(bb) >= tgt - 1e-9

            if not filled:
                all_hit = False

        pos["last_mark_update_utc"] = now.replace(microsecond=0).isoformat()
        if ecmwf_bulletin_recheck:
            pos["last_ecmwf_recheck_utc"] = now.replace(microsecond=0).isoformat()
            pos["bulletins_seen_since_open"] = int(pos.get("bulletins_seen_since_open") or 0) + 1

        _journal(
            journal_path,
            "position_mark_updated",
            {
                "event_slug": slug,
                "unrealized_estimate": _leg_sum_unrealized(pos),
                "ecmwf_bulletin_recheck": ecmwf_bulletin_recheck,
            },
        )

        if not paper_enable_virtual_sell_plan() or not legs:
            continue

        # --- виртуальный выход ---
        if close_mode == "all_legs_hit":
            if not all_hit:
                continue
            fee_out_sum = 0.0
            for leg in legs:
                tgt = float(leg.get("target_sell_price") or 0)
                ctr = float(leg.get("estimated_contracts") or 0)
                pr = tgt * ctr
                px_fee = tgt
                fee_out_sum += fee_round(fee_usd_exit_for_mode(pr, px_fee, mode=exit_mode))
            proceeds = 0.0
            for leg in legs:
                tgt = float(leg.get("target_sell_price") or 0)
                ctr = float(leg.get("estimated_contracts") or 0)
                proceeds += tgt * ctr
            entry_cost = float(pos.get("total_allocated_usd") or 0)
            fee_in = float(pos.get("total_entry_fee_estimated") or 0)
            pnl = proceeds - entry_cost - fee_in - fee_out_sum
            realized += pnl
            cash += proceeds - fee_out_sum
            pos["status"] = "closed"
            pos["closed_at_utc"] = now.replace(microsecond=0).isoformat()
            pos["realized_pnl"] = round(pnl, 4)
            pos["exit_kind"] = "all_legs_hit"
            closed.append(pos)
            to_del.append(slug)
            ex["all_legs_hit"] = int(ex.get("all_legs_hit") or 0) + 1
            _journal(
                journal_path,
                "position_closed",
                {
                    "event_slug": slug,
                    "paper_trade_id": pos.get("paper_trade_id"),
                    "realized_pnl": round(pnl, 4),
                    "cash_after": round(cash, 4),
                    "exit_kind": "all_legs_hit",
                },
            )
            stats["paper_exits_today_msk"] = int(stats.get("paper_exits_today_msk") or 0) + 1
            try:
                from paper_telegram_messages import format_paper_position_closed_message

                m.send_paper_telegram_safe(
                    format_paper_position_closed_message(
                        display_name_en=str(pos.get("display_name_en") or "?"),
                        event_slug=slug,
                        exit_kind="all_legs_hit",
                        realized_pnl=float(pnl),
                        cash_after=float(cash),
                    ),
                    event_slug=slug,
                )
            except Exception:
                pass
            continue

        # independent_leg_exit: закрываем ноги по одной при достижении target
        for leg in legs:
            if leg.get("status") == "closed_virtual":
                continue
            mk = _find_market_for_leg(markets, leg)
            if mk is None:
                continue
            liq = m._yes_price_and_liquidity(mk)
            mark = float(liq["yes"])
            bb = liq.get("best_bid")
            tgt = float(leg.get("target_sell_price") or 0)
            ctr = float(leg.get("estimated_contracts") or 0)
            filled = False
            if exit_mode == "taker_like":
                filled = mark >= tgt - 1e-9
            else:
                filled = bb is not None and float(bb) >= tgt - 1e-9
            if not filled:
                continue

            proceeds = tgt * ctr
            px_fee = float(bb) if (exit_mode == "maker_like" and bb is not None) else mark
            fee_out_leg = fee_round(fee_usd_exit_for_mode(proceeds, px_fee, mode=exit_mode))
            fee_in_leg = float(leg.get("entry_fee_allocated") or 0)
            leg_cost = float(leg.get("allocated_usd") or 0)
            leg_pnl = proceeds - leg_cost - fee_in_leg - fee_out_leg
            realized += leg_pnl
            cash += proceeds - fee_out_leg
            leg["status"] = "closed_virtual"
            leg["closed_at_utc"] = now.replace(microsecond=0).isoformat()
            leg["virtual_realized_pnl"] = round(leg_pnl, 4)
            leg["exit_fee_paid"] = fee_out_leg
            ex["independent_leg_target_hit"] = int(ex.get("independent_leg_target_hit") or 0) + 1
            _journal(
                journal_path,
                "leg_closed_virtual",
                {
                    "event_slug": slug,
                    "paper_trade_id": pos.get("paper_trade_id"),
                    "leg_key": leg.get("leg_key"),
                    "leg_realized_pnl": round(leg_pnl, 4),
                    "cash_after": round(cash, 4),
                },
            )
            stats["paper_exits_today_msk"] = int(stats.get("paper_exits_today_msk") or 0) + 1
            try:
                from paper_telegram_messages import format_paper_leg_exit_message

                m.send_paper_telegram_safe(
                    format_paper_leg_exit_message(
                        display_name_en=str(pos.get("display_name_en") or "?"),
                        event_slug=slug,
                        leg_key=leg.get("leg_key"),
                        bucket_label=leg.get("bucket_label"),
                        exit_reason="independent_leg_target_hit",
                        leg_realized_pnl=float(leg_pnl),
                        cash_after=float(cash),
                    ),
                    event_slug=slug,
                )
            except Exception:
                pass

        open_legs = [lg for lg in legs if lg.get("status") != "closed_virtual"]
        if open_legs:
            continue

        pos["status"] = "closed"
        pos["closed_at_utc"] = now.replace(microsecond=0).isoformat()
        total_leg_pnl = sum(float(lg.get("virtual_realized_pnl") or 0) for lg in legs)
        pos["realized_pnl"] = round(total_leg_pnl, 4)
        pos["exit_kind"] = "independent_all_legs_done"
        closed.append(pos)
        to_del.append(slug)
        _journal(
            journal_path,
            "position_closed",
            {
                "event_slug": slug,
                "paper_trade_id": pos.get("paper_trade_id"),
                "realized_pnl": round(total_leg_pnl, 4),
                "cash_after": round(cash, 4),
                "exit_kind": "independent_all_legs_done",
            },
        )
        try:
            from paper_telegram_messages import format_paper_position_closed_message

            m.send_paper_telegram_safe(
                format_paper_position_closed_message(
                    display_name_en=str(pos.get("display_name_en") or "?"),
                    event_slug=slug,
                    exit_kind="independent_all_legs_done",
                    realized_pnl=float(total_leg_pnl),
                    cash_after=float(cash),
                    extra_note="Все ноги закрыты (см. отдельные сообщения по ногам).",
                ),
                event_slug=slug,
            )
        except Exception:
            pass

    for slug in to_del:
        open_map.pop(slug, None)

    portfolio["open_positions"] = open_map
    portfolio["closed_positions"] = closed[-500:]
    portfolio["realized_pnl"] = round(realized, 4)
    portfolio["current_cash"] = round(cash, 4)
    portfolio["stats"]["closed_count"] = len(closed)


def _leg_sum_unrealized(pos: dict[str, Any]) -> float:
    s = 0.0
    for leg in pos.get("legs") or []:
        if leg.get("status") == "closed_virtual":
            continue
        u = leg.get("current_unrealized_pnl_estimate")
        if isinstance(u, (int, float)):
            s += float(u)
    return round(s, 4)


def init_paper_if_missing() -> None:
    """Создать paper_portfolio.json с стартовым балансом, если файла нет."""
    from paper_settings import paper_portfolio_path
    import os

    p = paper_portfolio_path()
    if os.path.isfile(p):
        return
    data = load_portfolio(p)
    if float(data.get("starting_balance") or 0) <= 0:
        data["starting_balance"] = paper_start_balance()
        data["current_cash"] = data["starting_balance"]
    save_portfolio(data, p)
