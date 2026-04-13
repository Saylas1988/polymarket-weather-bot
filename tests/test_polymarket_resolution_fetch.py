"""Тесты парсинга METAR и (опционально) сетевых fetch без обязательного интернета."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from polymarket_resolution_fetch import _parse_metar_temperature_c


class TestMetarTemp(unittest.TestCase):
    def test_cavok(self) -> None:
        r = "METAR UUWW 150600Z VRB01MPS CAVOK 20/11 Q1015"
        self.assertEqual(_parse_metar_temperature_c(r), 20.0)

    def test_negative(self) -> None:
        r = "METAR UUWW 150600Z VRB01MPS CAVOK M05/M07 Q1015"
        self.assertEqual(_parse_metar_temperature_c(r), -5.0)

    def test_eglc_auto(self) -> None:
        r = "METAR EGLC 150020Z AUTO 20007KT 180V250 9999 NCD 12/09 Q0998="
        self.assertEqual(_parse_metar_temperature_c(r), 12.0)


if __name__ == "__main__":
    unittest.main()
