"""Тесты settlement и сопоставления winning bucket."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper_settlement import leg_is_winning


class TestLegWinner(unittest.TestCase):
    def test_condition_id(self) -> None:
        leg = {"condition_id": "0xabc", "question": "Will it be 10–12°C?"}
        self.assertTrue(leg_is_winning(leg, winning_label="x", winning_condition_id="0xabc"))

    def test_question_norm(self) -> None:
        leg = {"condition_id": "", "question": "Will the high be 10–12°C on Jun 15?"}
        w = "Will the high be 10–12°C on Jun 15?"
        self.assertTrue(leg_is_winning(leg, winning_label=w, winning_condition_id=None))


if __name__ == "__main__":
    unittest.main()
