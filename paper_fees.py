"""
Обёртка совместимости: расчёт комиссий делегируется в paper_fee_logic.

Старый API fee_usd_from_notional(notional, fee_bps) сохранён для скриптов,
которые передают уже готовые bps; для paper engine используйте paper_fee_logic.
"""

from __future__ import annotations

from paper_fee_logic import (
    fee_usd_exit_for_mode,
    fee_usd_exit_maker_like,
    fee_usd_exit_taker_like,
    fee_usd_taker_notional,
    round_fee,
)
from paper_settings import paper_fee_taker_base_bps


def fee_usd_from_notional(notional_usd: float, fee_bps: float) -> float:
    """Плоская модель по готовым bps (legacy / тесты)."""
    if notional_usd <= 0 or fee_bps <= 0:
        return 0.0
    return max(0.0, float(notional_usd) * float(fee_bps) / 10000.0)


__all__ = [
    "fee_usd_from_notional",
    "fee_usd_taker_notional",
    "fee_usd_exit_maker_like",
    "fee_usd_exit_taker_like",
    "fee_usd_exit_for_mode",
    "round_fee",
    "paper_fee_taker_base_bps",
]
