"""
Этап 3: paper trading — параметры через env.
"""

from __future__ import annotations

import os


def _b(name: str, default: str) -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


def _f(name: str, default: str) -> float:
    return float(os.environ.get(name, default).strip())


def _i(name: str, default: str) -> int:
    return int(os.environ.get(name, default).strip())


def paper_trading_enabled() -> bool:
    return _b("PAPER_TRADING_ENABLED", "0")


def paper_start_balance() -> float:
    return _f("PAPER_START_BALANCE", "500")


def paper_max_risk_per_trade_pct() -> float:
    """Доля банка на одну идею, например 0.08 = 8%."""
    return _f("PAPER_MAX_RISK_PER_TRADE_PCT", "0.08")


def paper_max_new_positions_per_cycle() -> int:
    return _i("PAPER_MAX_NEW_POSITIONS_PER_CYCLE", "2")


def paper_signal_ttl_minutes() -> float:
    return _f("PAPER_SIGNAL_TTL_MINUTES", "90")


def paper_portfolio_path() -> str:
    base = os.path.dirname(os.path.abspath(__file__))
    return os.environ.get("PAPER_PORTFOLIO_PATH", os.path.join(base, "paper_portfolio.json"))


def paper_trade_journal_path() -> str:
    base = os.path.dirname(os.path.abspath(__file__))
    return os.environ.get("PAPER_TRADE_JOURNAL_PATH", os.path.join(base, "paper_trade_journal.jsonl"))


def paper_exit_mode() -> str:
    """
    maker_like — выход оцениваем по более консервативной цене (bid).
    taker_like — выход по агрессивной модели (ближе к ask / mid).
    """
    return os.environ.get("PAPER_EXIT_MODE", "maker_like").strip().lower()


def paper_close_mode() -> str:
    """
    Режим виртуального закрытия позиции (первая неделя: independent_leg_exit).
    - independent_leg_exit — каждая нога закрывается при своём target (без trim).
    - all_legs_hit — устаревшее: вся позиция только когда все ноги достигли target.
    """
    return os.environ.get("PAPER_CLOSE_MODE", "independent_leg_exit").strip().lower()


def paper_allow_reentry_same_event() -> bool:
    return _b("PAPER_ALLOW_REENTRY_SAME_EVENT", "0")


def paper_enable_virtual_sell_plan() -> bool:
    return _b("PAPER_ENABLE_VIRTUAL_SELL_PLAN", "1")


def paper_telegram_notifications_enabled() -> bool:
    """Короткие уведомления paper в Telegram (вход/выход/сводки)."""
    return _b("PAPER_TELEGRAM_NOTIFY", "1")


def paper_reports_dir() -> str:
    base = os.path.dirname(os.path.abspath(__file__))
    d = os.environ.get("PAPER_REPORTS_DIR", os.path.join(base, "paper_reports"))
    return d


def paper_manual_journal_path() -> str:
    """
    Текстовый автожурнал входов (человекочитаемый), не JSON.
    На Railway задайте PAPER_MANUAL_JOURNAL_PATH=/data/paper_manual_journal.txt (volume).
    """
    base = os.path.dirname(os.path.abspath(__file__))
    return os.environ.get("PAPER_MANUAL_JOURNAL_PATH", os.path.join(base, "paper_manual_journal.txt"))


def paper_verification_state_path() -> str:
    """JSON: какие event_slug уже прошли автоверификацию итога рынка (дедуп)."""
    base = os.path.dirname(os.path.abspath(__file__))
    return os.environ.get("PAPER_VERIFICATION_STATE_PATH", os.path.join(base, "paper_verification_state.json"))


def market_verification_enabled() -> bool:
    """Автоверификация фактической tmax + победившего бакета после даты события."""
    return _b("MARKET_VERIFY_ENABLED", "1")


def market_verify_openmeteo_debug() -> bool:
    """Добавить в запись verification поля openmeteo_reference_temperature_c (не для итога рынка)."""
    return _b("MARKET_VERIFY_OPENMETEO_DEBUG", "0")


def market_verify_min_full_days_after_event() -> int:
    """
    Дополнительное ожидание полных локальных суток после дня события.
    0 — верифицировать, как только календарный день в городе уже следующий (D+1 локально).
    1 — ждать ещё один день (полезно, если архив обновляется с задержкой).
    """
    return max(0, _i("MARKET_VERIFY_MIN_FULL_DAYS_AFTER_EVENT", "0"))


# --- Комиссии (paper_fee_logic): базовый taker bps и форма от цены ---
def paper_fee_taker_base_bps() -> float:
    """Базовые bps до поправки phi(p); fallback на PAPER_FEE_ENTRY_BPS."""
    raw = os.environ.get("PAPER_FEE_TAKER_BASE_BPS")
    if raw is not None and str(raw).strip() != "":
        return float(str(raw).strip())
    raw2 = os.environ.get("PAPER_FEE_ENTRY_BPS")
    if raw2 is not None and str(raw2).strip() != "":
        return float(str(raw2).strip())
    return 200.0


def paper_fee_phi_weight() -> float:
    """Насколько усиливаем комиссию около p=0.5 (0 = плоские bps как раньше)."""
    return _f("PAPER_FEE_PHI_WEIGHT", "0.35")


def paper_fee_maker_exit_discount() -> float:
    """Множитель к эффективным bps при maker_like exit (0..1)."""
    return _f("PAPER_FEE_MAKER_EXIT_DISCOUNT", "0.55")


# Обратная совместимость: старые имена env
def paper_fee_entry_bps() -> float:
    return paper_fee_taker_base_bps()


def paper_fee_exit_bps() -> float:
    """Устаревшее плоское значение; для отчётов — см. paper_fee_logic."""
    return _f("PAPER_FEE_EXIT_BPS", str(paper_fee_taker_base_bps()))


def paper_min_allocation_usd() -> float:
    return _f("PAPER_MIN_ALLOCATION_USD", "1.0")


def signal_logic_version() -> str:
    return os.environ.get("PAPER_SIGNAL_LOGIC_VERSION", "2.0").strip()


def allocation_logic_version() -> str:
    return os.environ.get("PAPER_ALLOCATION_LOGIC_VERSION", "4.0-heuristic").strip()


def fee_logic_version() -> str:
    return os.environ.get("PAPER_FEE_LOGIC_VERSION", "4.0-weather-phi").strip()


def exit_logic_version() -> str:
    return os.environ.get("PAPER_EXIT_LOGIC_VERSION", "4.0-independent-legs").strip()


# ---------------------------------------------------------------------------
# Week 1 paper test — эталонные правила (снимок для отчётов; реальные значения из env)
# ---------------------------------------------------------------------------
WEEK1_PAPER_TEST_FREEZE: dict[str, object] = {
    "label": "week1_paper_2026",
    "starting_balance_usd": 500,
    "max_risk_per_trade_pct": 0.08,
    "max_new_positions_per_cycle": 2,
    "signal_ttl_minutes": 90,
    "paper_close_mode": "independent_leg_exit",
    "paper_exit_mode_for_fills": "maker_like",
    "allocator_logic_version_ref": "4.0-heuristic",
    "fee_logic_version_ref": "4.0-weather-phi",
    "exit_logic_version_ref": "4.0-independent-legs",
    "no_trim_cut": True,
    "no_add_rebalance": True,
    "one_event_one_position": True,
}


def week1_paper_test_freeze_snapshot() -> dict[str, object]:
    """Сводка «заморозки» первой недели + фактические версии/параметры из env."""
    return {
        **WEEK1_PAPER_TEST_FREEZE,
        "effective": {
            "starting_balance": paper_start_balance(),
            "max_risk_per_trade_pct": paper_max_risk_per_trade_pct(),
            "max_new_positions_per_cycle": paper_max_new_positions_per_cycle(),
            "signal_ttl_minutes": paper_signal_ttl_minutes(),
            "paper_close_mode": paper_close_mode(),
            "paper_exit_mode": paper_exit_mode(),
            "allocator_logic_version": allocation_logic_version(),
            "fee_logic_version": fee_logic_version(),
            "exit_logic_version": exit_logic_version(),
        },
    }
