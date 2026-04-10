"""
Параметры этапа 2: ликвидность, журнал сигналов.
Переопределение через env (и опционально per-depth суффикс _D1 / _D2 / _D3).
"""

from __future__ import annotations

import os


def _f(name: str, default: str) -> float:
    return float(os.environ.get(name, default).strip())


def _b(name: str, default: str) -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


def liquidity_max_spread_main(depth: int) -> float:
    """Максимально допустимый спред (ask - bid) по главному бакету YES."""
    d = os.environ.get(f"LIQUIDITY_MAX_SPREAD_MAIN_D{depth}")
    if d is not None and d.strip() != "":
        return float(d.strip())
    return _f("LIQUIDITY_MAX_SPREAD_MAIN", "0.22")


def liquidity_min_volume_main(depth: int) -> float:
    """Минимальный объём рынка главного бакета (Gamma volumeNum/volume)."""
    d = os.environ.get(f"LIQUIDITY_MIN_VOLUME_MAIN_D{depth}")
    if d is not None and d.strip() != "":
        return float(d.strip())
    return _f("LIQUIDITY_MIN_VOLUME_MAIN", "300")


def liquidity_min_neighbor_volume(depth: int) -> float:
    """0 = не проверять соседей. Иначе минимум объёма на каждом из двух соседей (если есть)."""
    d = os.environ.get(f"LIQUIDITY_MIN_NEIGHBOR_VOLUME_D{depth}")
    if d is not None and d.strip() != "":
        return float(d.strip())
    return _f("LIQUIDITY_MIN_NEIGHBOR_VOLUME", "0")


def require_best_ask_for_entry() -> bool:
    return _b("REQUIRE_BEST_ASK_FOR_ENTRY", "1")


def signal_journal_path() -> str:
    base = os.path.dirname(os.path.abspath(__file__))
    return os.environ.get("SIGNAL_JOURNAL_PATH", os.path.join(base, "signal_journal.jsonl"))
