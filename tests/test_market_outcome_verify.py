"""Юнит-тесты автоверификации итога (без сети)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_outcome_verify import pick_winning_bucket


class TestWinningBucket(unittest.TestCase):
    def test_contains_middle_c(self) -> None:
        ladder = [
            {"question": "≤10°C", "low": None, "high": 10.0},
            {"question": "11–15°C", "low": 11.0, "high": 15.0},
            {"question": "≥16°C", "low": 16.0, "high": None},
        ]
        w = pick_winning_bucket(12.5, ladder, "C")
        self.assertIn("11", w["winning_bucket_label"])

    def test_fallback_nearest(self) -> None:
        ladder = [
            {"question": "A", "low": 0.0, "high": 5.0},
            {"question": "B", "low": 10.0, "high": 15.0},
        ]
        w = pick_winning_bucket(7.0, ladder, "C")
        self.assertEqual(w["winning_bucket_label"], "A")


if __name__ == "__main__":
    unittest.main()
