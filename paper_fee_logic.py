"""
Слой оценки комиссий для paper trading (погодные бинарные рынки Polymarket).

Цель — не «production fee engine для всех рынков», а прозрачная и достаточно
честная модель для симуляции: комиссия зависит от цены YES и от сценария
исполнения (taker entry; maker_like vs taker_like exit).

Упрощения (честно):
- Используется одна семья формул на базе «эффективных bps», а не точный
  match с внутренним fee schedule CLOB Polymarket (он может меняться и
  зависеть от аккаунта/рынка).
- Нет отдельного учёта минимальной комиссии в USD — только процент от номинала.
- Спред учитывается косвенно: для maker_like exit берётся цена bid; для
  taker_like — mid/ask в зависимости от переданного аргумента.
- Не моделируются rebates / оракульные выплаты — только обмен по книге.

Версия логики: см. fee_logic_version() в paper_settings.
"""

from __future__ import annotations

from typing import Literal

from paper_settings import (
    paper_fee_maker_exit_discount,
    paper_fee_phi_weight,
    paper_fee_taker_base_bps,
)

ExecutionExit = Literal["maker_like", "taker_like"]


def _clamp01(p: float) -> float:
    return min(max(float(p), 1e-9), 1.0 - 1e-9)


def phi_binary_liquidity(p_yes: float) -> float:
    """
    Форма «ликвидностной» нагрузки для бинарного YES: максимум у p=0.5, нули у 0/1.
    Удобно масштабировать taker fee без резких краёв.
    """
    p = _clamp01(p_yes)
    return 4.0 * p * (1.0 - p)


def effective_taker_bps_at_price(p_yes: float) -> float:
    """
    Эффективные bps taker-стиля в зависимости от цены.
    base * (1 + w * phi) — выше около 0.5.
    """
    base = float(paper_fee_taker_base_bps())
    w = float(paper_fee_phi_weight())
    return max(0.0, base * (1.0 + w * phi_binary_liquidity(p_yes)))


def fee_usd_taker_notional(notional_usd: float, price_yes: float) -> float:
    """Вход taker: номинал покупки YES по ask-цене ~ price_yes."""
    if notional_usd <= 0:
        return 0.0
    bps = effective_taker_bps_at_price(price_yes)
    return max(0.0, float(notional_usd) * bps / 10000.0)


def fee_usd_exit_maker_like(notional_usd: float, price_yes_bid: float) -> float:
    """
    Выход «как лимит у bid» — консервативнее: эффективная ставка ниже taker.
    """
    if notional_usd <= 0:
        return 0.0
    bps = effective_taker_bps_at_price(price_yes_bid) * float(paper_fee_maker_exit_discount())
    return max(0.0, float(notional_usd) * bps / 10000.0)


def fee_usd_exit_taker_like(notional_usd: float, price_yes: float) -> float:
    """Выход агрессивный (ближе к taker / снятие ликвидности)."""
    if notional_usd <= 0:
        return 0.0
    bps = effective_taker_bps_at_price(price_yes)
    return max(0.0, float(notional_usd) * bps / 10000.0)


def fee_usd_exit_for_mode(
    notional_usd: float,
    price_for_fee: float,
    *,
    mode: ExecutionExit,
) -> float:
    """Единая точка для paper exit_mode (maker_like | taker_like)."""
    if mode == "taker_like":
        return fee_usd_exit_taker_like(notional_usd, price_for_fee)
    return fee_usd_exit_maker_like(notional_usd, price_for_fee)


def round_fee(x: float) -> float:
    return round(float(x), 4)
