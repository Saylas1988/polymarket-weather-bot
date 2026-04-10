"""JSONL журнал paper trading (отдельно от signal_journal)."""

from __future__ import annotations

import json
import logging
from typing import Any

log = logging.getLogger("rainmaker")


def append_paper_trade_record(record: dict[str, Any], *, path: str) -> None:
    try:
        line = json.dumps(record, ensure_ascii=False, default=str) + "\n"
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception as e:
        log.warning("paper trade journal write failed: %s", e)
