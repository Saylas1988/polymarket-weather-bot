#!/usr/bin/env python3
"""
Сброс paper-state и связанных журналов перед новым недельным тестом.

Использует те же env, что и бот (PAPER_PORTFOLIO_PATH, PAPER_TRADE_JOURNAL_PATH,
PAPER_REPORTS_DIR, SIGNAL_JOURNAL_PATH). На Railway задайте их перед запуском
или выполните скрипт в окружении сервиса.

Безопасность: по умолчанию только dry-run; реальное удаление — флаг --yes.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys


def _repo_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main() -> int:
    root = _repo_root()
    if root not in sys.path:
        sys.path.insert(0, root)

    from paper_settings import paper_portfolio_path, paper_reports_dir, paper_trade_journal_path
    from signal_config import signal_journal_path

    ap = argparse.ArgumentParser(description="Сброс paper_portfolio, журналов и paper_reports")
    ap.add_argument(
        "--yes",
        action="store_true",
        help="выполнить удаление (без этого только показать пути)",
    )
    args = ap.parse_args()

    portfolio = os.path.abspath(paper_portfolio_path())
    trade_j = os.path.abspath(paper_trade_journal_path())
    sig_j = os.path.abspath(signal_journal_path())
    reports = os.path.abspath(paper_reports_dir())

    print("RainMakerBot — сброс paper-state")
    print(f"  paper_portfolio.json  -> {portfolio}")
    print(f"  paper_trade_journal   -> {trade_j}")
    print(f"  signal_journal        -> {sig_j}")
    print(f"  paper_reports/        -> очистить {reports}")

    if not args.yes:
        print("\nDry-run: ничего не удалено. Повторите с --yes для выполнения.")
        return 0

    removed: list[str] = []

    def rm_file(p: str) -> None:
        if os.path.isfile(p):
            os.remove(p)
            removed.append(p)

    rm_file(portfolio)
    rm_file(trade_j)
    rm_file(sig_j)

    if os.path.isdir(reports):
        shutil.rmtree(reports)
        removed.append(f"{reports}/ (вся папка)")
    os.makedirs(reports, exist_ok=True)
    print(f"\nСоздана пустая папка: {reports}")

    if removed:
        print("\nУдалено:")
        for x in removed:
            print(f"  - {x}")
    else:
        print("\nФайлы уже отсутствовали (портфель/журналы); paper_reports пересоздана.")

    print(
        "\nПри следующем запуске бот создаст paper_portfolio.json через init_paper_if_missing "
        "(стартовый баланс из PAPER_START_BALANCE)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
