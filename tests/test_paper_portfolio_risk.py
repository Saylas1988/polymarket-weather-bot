"""Тесты portfolio risk engine (без полного paper round)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper_portfolio_risk import (
    aggregate_open_risk,
    determine_risk_mode,
    portfolio_risk_allows_new_entry,
)


def _pos(city: str, date: str, usd: float) -> dict:
    return {
        "city_key": city,
        "event_date": date,
        "total_allocated_usd": usd,
        "legs": [],
    }


class TestAggregate(unittest.TestCase):
    def test_totals(self) -> None:
        p = {
            "starting_balance": 500.0,
            "unrealized_pnl_estimate": 0.0,
            "open_positions": {
                "a": _pos("london", "2026-05-01", 40),
                "b": _pos("london", "2026-05-02", 30),
                "c": _pos("paris", "2026-05-01", 20),
            },
        }
        s = aggregate_open_risk(p)
        self.assertEqual(s["open_events_count"], 3)
        self.assertAlmostEqual(s["total_open_exposure_usd"], 90.0)
        self.assertAlmostEqual(s["by_city"]["london"]["exposure_usd"], 70.0)
        self.assertEqual(s["by_city"]["london"]["open_events"], 2)


class TestDrawdownMode(unittest.TestCase):
    def test_pause(self) -> None:
        p = {"starting_balance": 500.0, "unrealized_pnl_estimate": -40.0}
        with patch("paper_portfolio_risk.paper_unrealized_drawdown_pause_pct", return_value=0.07):
            with patch("paper_portfolio_risk.paper_unrealized_drawdown_hard_pct", return_value=0.12):
                self.assertEqual(determine_risk_mode(p), "drawdown_pause")

    def test_hard(self) -> None:
        p = {"starting_balance": 500.0, "unrealized_pnl_estimate": -70.0}
        with patch("paper_portfolio_risk.paper_unrealized_drawdown_pause_pct", return_value=0.07):
            with patch("paper_portfolio_risk.paper_unrealized_drawdown_hard_pct", return_value=0.12):
                self.assertEqual(determine_risk_mode(p), "hard_reduction")


class TestGate(unittest.TestCase):
    def test_max_events(self) -> None:
        p = {
            "starting_balance": 500.0,
            "unrealized_pnl_estimate": 0.0,
            "open_positions": {f"k{i}": _pos("x", "2026-06-01", 1) for i in range(3)},
        }
        with patch("paper_portfolio_risk.paper_max_open_events", return_value=3):
            with patch("paper_portfolio_risk.paper_max_open_events_per_city", return_value=99):
                with patch("paper_portfolio_risk.paper_max_total_open_exposure_pct", return_value=1.0):
                    with patch("paper_portfolio_risk.paper_max_city_exposure_pct", return_value=1.0):
                        with patch("paper_portfolio_risk.paper_max_same_date_exposure_pct", return_value=1.0):
                            ok, reason, _ = portfolio_risk_allows_new_entry(
                                p, city_key="y", event_date="2026-06-02", proposed_budget_usd=10.0
                            )
                            self.assertFalse(ok)
                            self.assertEqual(reason, "portfolio_risk_max_open_events")


if __name__ == "__main__":
    unittest.main()
