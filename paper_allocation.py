"""
Allocator для paper trading: распределение банка по ногам с учётом цен, комиссий и ликвидности.

Версия логики: allocation_logic_version() в paper_settings.

Упрощения:
- Для соседних ног нет отдельного ensemble p в старых данных — используем p_lower/p_upper
  из evaluate (если есть) иначе только рыночные yes и ликвидность.
- «Net edge» — оценка под виртуальные target_sell, не ожидаемая доходность в смысле статистики.
"""

from __future__ import annotations

from typing import Any

from paper_fee_logic import (
    fee_usd_exit_for_mode,
    fee_usd_taker_notional,
    round_fee,
)
from paper_settings import (
    allocation_logic_version,
    exit_logic_version,
    fee_logic_version,
    paper_exit_mode,
    paper_min_allocation_usd,
)


def _sell_target_mult(depth: int) -> float:
    return {1: 0.95, 2: 0.90, 3: 0.85}.get(int(depth), 0.85)


def _leg_entry_px(bucket: dict[str, Any]) -> float:
    ba = bucket.get("best_ask")
    if ba is not None:
        return float(ba)
    return float(bucket.get("yes") or 0)


def _spread(bucket: dict[str, Any]) -> float:
    bb, ba = bucket.get("best_bid"), bucket.get("best_ask")
    if bb is not None and ba is not None:
        return max(0.0, float(ba) - float(bb))
    return 0.08


def _neighbor_quality(
    bucket: dict[str, Any],
    *,
    p_ensemble: float | None,
    gap_entry_main: float | None,
) -> tuple[float, list[str]]:
    """
    Возвращает factor in [0,1] и причины порезки. 0 = нога отбрасывается.
    """
    reasons: list[str] = []
    vol = float(bucket.get("volume") or 0)
    sp = _spread(bucket)
    yes = float(bucket.get("yes") or 0)

    # ликвидность
    if vol < 800:
        reasons.append("low_volume")
    vol_score = min(1.0, vol / (vol + 4000.0))

    # спред
    sp_bad = min(1.0, 0.12 / max(sp, 0.02))
    if sp > 0.14:
        reasons.append("wide_spread")

    # дорогая «хедж»-нога
    price_score = max(0.0, 1.0 - max(0.0, yes - 0.22) / 0.55)
    if yes > 0.42:
        reasons.append("neighbor_yes_high")

    # ensemble: сосед почти не поддержан моделью
    if p_ensemble is not None and p_ensemble < 0.04:
        reasons.append("p_neighbor_tiny")
        return 0.0, reasons

    # слабый gap у главной — соседям меньше доверия
    gap_pen = 1.0
    if gap_entry_main is not None and gap_entry_main < 0.08:
        gap_pen = 0.55
        reasons.append("weak_main_gap")

    raw = vol_score * sp_bad * price_score * gap_pen
    raw = max(0.0, min(1.0, raw))
    return raw, reasons


def _target_px_for_leg(
    bucket: dict[str, Any],
    *,
    is_main: bool,
    p_main: float,
    depth: int,
) -> float:
    if is_main:
        t = round(min(0.99, float(p_main) * _sell_target_mult(depth)), 4)
        return max(t, float(bucket.get("yes") or 0) * 0.5)
    y = float(bucket.get("yes") or 0)
    return round(min(0.98, y * 1.08), 4)


def generate_bucket_allocation(
    *,
    total_budget_usd: float,
    structure_type: str,
    res: dict[str, Any],
    depth: int,
) -> dict[str, Any]:
    if total_budget_usd <= 0 or structure_type == "no_trade":
        return {
            "allocation_total_usd": 0.0,
            "main_bucket_usd": 0.0,
            "lower_bucket_usd": 0.0,
            "upper_bucket_usd": 0.0,
            "max_entry_price_per_leg": {},
            "fee_estimate_entry": 0.0,
            "fee_estimate_exit_assumed": 0.0,
            "net_edge_estimate": None,
            "structure_type": structure_type,
            "structure_type_effective": "no_trade",
            "allocation_logic_version": allocation_logic_version(),
            "fee_logic_version": fee_logic_version(),
            "exit_logic_version": exit_logic_version(),
            "allocator_notes": [],
            "neighbor_cuts": [],
        }

    p_main = float(res.get("p_main") or 0)
    gap_entry = res.get("gap_entry")
    gap_entry_f = float(gap_entry) if isinstance(gap_entry, (int, float)) else None
    em = paper_exit_mode()
    exit_mode = em if em in ("maker_like", "taker_like") else "maker_like"

    main = res.get("main") or {}
    low_b = res.get("low")
    high_b = res.get("high")

    p_lower = res.get("p_lower")
    p_upper = res.get("p_upper")
    p_low_f = float(p_lower) if isinstance(p_lower, (int, float)) else None
    p_up_f = float(p_upper) if isinstance(p_upper, (int, float)) else None

    if structure_type == "single_bucket":
        m_usd = max(0.0, float(total_budget_usd))
        ep = _leg_entry_px(main)
        if ep <= 0:
            m_usd = 0.0
        fee_in = round_fee(fee_usd_taker_notional(m_usd, ep))
        tgt = _target_px_for_leg(main, is_main=True, p_main=p_main, depth=depth)
        contracts = (m_usd / ep) if ep > 0 else 0.0
        exit_notional = tgt * contracts
        fee_out = round_fee(fee_usd_exit_for_mode(exit_notional, tgt, mode=exit_mode))
        net = exit_notional - fee_out - (m_usd + fee_in)

        return {
            "allocation_total_usd": round(m_usd, 2),
            "main_bucket_usd": round(m_usd, 2),
            "lower_bucket_usd": 0.0,
            "upper_bucket_usd": 0.0,
            "max_entry_price_per_leg": {"main": ep},
            "fee_estimate_entry": fee_in,
            "fee_estimate_exit_assumed": fee_out,
            "net_edge_estimate": round(net, 4),
            "target_sell_prices": {"main": tgt},
            "structure_type": structure_type,
            "structure_type_effective": "single_bucket",
            "allocation_logic_version": allocation_logic_version(),
            "fee_logic_version": fee_logic_version(),
            "exit_logic_version": exit_logic_version(),
            "allocator_notes": ["single_bucket_all_to_main"],
            "neighbor_cuts": [],
        }

    if low_b is None or high_b is None or main is None:
        return generate_bucket_allocation(
            total_budget_usd=total_budget_usd,
            structure_type="single_bucket",
            res=res,
            depth=depth,
        )

    # --- ladder_3 heuristic allocator ---
    notes: list[str] = []
    cuts: list[str] = []

    q_low, rlow = _neighbor_quality(low_b, p_ensemble=p_low_f, gap_entry_main=gap_entry_f)
    q_high, rhigh = _neighbor_quality(high_b, p_ensemble=p_up_f, gap_entry_main=gap_entry_f)
    if rlow:
        cuts.append(f"lower:{','.join(rlow)}")
    if rhigh:
        cuts.append(f"upper:{','.join(rhigh)}")

    # Prior masses: ensemble if present else fallback to 70/15/15 style prior
    if p_low_f is not None and p_up_f is not None:
        s = p_low_f + float(p_main) + p_up_f
        if s > 1e-9:
            w_low, w_main, w_high = p_low_f / s, float(p_main) / s, p_up_f / s
        else:
            w_low, w_main, w_high = 0.15, 0.70, 0.15
    else:
        w_low, w_main, w_high = 0.15, 0.70, 0.15
        notes.append("no_neighbor_ensemble_used_prior_701515")

    w_low *= q_low
    w_high *= q_high
    # если оба соседа вырезаны — вся сумма в main (не «тупой» 70/15/15)
    if w_low + w_high < 1e-6:
        notes.append("neighbors_zeroed_all_to_main")
        return generate_bucket_allocation(
            total_budget_usd=total_budget_usd,
            structure_type="single_bucket",
            res=res,
            depth=depth,
        )

    s = w_low + w_main + w_high
    w_low, w_main, w_high = w_low / s, w_main / s, w_high / s

    low_usd = total_budget_usd * w_low
    main_usd = total_budget_usd * w_main
    high_usd = total_budget_usd * w_high

    min_leg = paper_min_allocation_usd()
    # подрезка крошечных ног
    if 0 < low_usd < min_leg:
        notes.append("lower_below_min_fold_to_main")
        main_usd += low_usd
        low_usd = 0.0
        cuts.append("lower:below_min_allocation")
    if 0 < high_usd < min_leg:
        notes.append("upper_below_min_fold_to_main")
        main_usd += high_usd
        high_usd = 0.0
        cuts.append("upper:below_min_allocation")

    if low_usd == 0 and high_usd == 0:
        return generate_bucket_allocation(
            total_budget_usd=total_budget_usd,
            structure_type="single_bucket",
            res=res,
            depth=depth,
        )

    buckets_order = [low_b, main, high_b]
    keys = ["lower", "main", "upper"]
    usds = [low_usd, main_usd, high_usd]

    max_px: dict[str, float | None] = {}
    targets: dict[str, float] = {}
    fee_in_total = 0.0
    fee_out_total = 0.0
    net_parts: list[float] = []

    for key, bucket, usd in zip(keys, buckets_order, usds, strict=True):
        if bucket is None or usd <= 0:
            max_px[key] = None
            continue
        ep = _leg_entry_px(bucket)
        max_px[key] = ep
        is_main = key == "main"
        tgt = _target_px_for_leg(bucket, is_main=is_main, p_main=p_main, depth=depth)
        targets[key] = tgt
        fi = round_fee(fee_usd_taker_notional(usd, ep))
        contracts = (usd / ep) if ep > 0 else 0.0
        exn = tgt * contracts
        fo = round_fee(fee_usd_exit_for_mode(exn, tgt, mode=exit_mode))
        fee_in_total += fi
        fee_out_total += fo
        net_parts.append(exn - fo - usd - fi)

    net_edge = sum(net_parts) if net_parts else None

    return {
        "allocation_total_usd": round(sum(usds), 2),
        "main_bucket_usd": round(main_usd, 2),
        "lower_bucket_usd": round(low_usd, 2),
        "upper_bucket_usd": round(high_usd, 2),
        "max_entry_price_per_leg": max_px,
        "fee_estimate_entry": round_fee(fee_in_total),
        "fee_estimate_exit_assumed": round_fee(fee_out_total),
        "net_edge_estimate": round(net_edge, 4) if net_edge is not None else None,
        "target_sell_prices": targets,
        "structure_type": structure_type,
        "structure_type_effective": "ladder_3",
        "allocation_logic_version": allocation_logic_version(),
        "fee_logic_version": fee_logic_version(),
        "exit_logic_version": exit_logic_version(),
        "allocator_notes": notes,
        "neighbor_cuts": cuts,
        "allocator_weights": {"lower": w_low, "main": w_main, "upper": w_high},
    }
