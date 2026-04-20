"""
Состояние paper portfolio (JSON).
"""

from __future__ import annotations

import json
import os
from copy import deepcopy
from typing import Any

from paper_settings import paper_portfolio_path, paper_start_balance


def default_portfolio() -> dict[str, Any]:
    bal = paper_start_balance()
    return {
        "version": 4,
        "starting_balance": bal,
        "current_cash": bal,
        "open_positions": {},
        "closed_positions": [],
        "realized_pnl": 0.0,
        "unrealized_pnl_estimate": 0.0,
        "last_updated_utc": None,
        "stats": {
            "total_signals_seen": 0,
            "total_signals_taken": 0,
            "total_signals_skipped": 0,
            "skipped_by_reason": {},
            "closed_count": 0,
            "opened_today_msk": None,
            "structure_entries": {},
            "allocator_forced_single_from_ladder": 0,
            "allocator_partial_cuts": 0,
            "exit_reasons": {},
            "paper_activity_date_msk": None,
            "paper_entries_today_msk": 0,
            "paper_exits_today_msk": 0,
            "paper_skipped_today_msk": 0,
            "settlements_completed": 0,
        },
    }


def load_portfolio(path: str | None = None) -> dict[str, Any]:
    p = path or paper_portfolio_path()
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return default_portfolio()
        # миграция минимальная
        v = int(data.get("version") or 3)
        if v < 4:
            data["version"] = 4
        data.setdefault("open_positions", {})
        data.setdefault("closed_positions", [])
        st = data.setdefault("stats", {})
        for k, v in default_portfolio()["stats"].items():
            st.setdefault(k, v)
        return data
    except FileNotFoundError:
        return default_portfolio()
    except Exception:
        return default_portfolio()


def save_portfolio(data: dict[str, Any], path: str | None = None) -> None:
    p = path or paper_portfolio_path()
    tmp = p + ".tmp"
    payload = deepcopy(data)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, p)
