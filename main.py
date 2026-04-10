"""
RainMakerBot — этапы 1-2.

Этап 1: отправить в Telegram "Бот запущен"
Этап 2: вывести в консоль max температуру (ECMWF/Open-Meteo) на 3 дня
"""

import asyncio
import datetime as dt
import logging
import os
import json
import sys
import threading
import traceback
from logging.handlers import TimedRotatingFileHandler
from zoneinfo import ZoneInfo
import calendar
import re
import math

import requests
import schedule
from signal_config import (
    liquidity_max_spread_main,
    liquidity_min_neighbor_volume,
    liquidity_min_volume_main,
    require_best_ask_for_entry,
    signal_journal_path,
)
from signal_journal import append_signal_journal_record, msk_now_iso, utc_now_iso
from station_config import (
    CITY_STATIONS,
    city_config_by_display_name,
    city_config_from_slug_token,
    iter_enabled_city_configs,
)
from verification_context import build_resolution_context
from openmeteo_ensemble_cache import (
    OpenMeteoEnsembleUnavailable,
    get_ensemble_daily_json_cached,
    log_ensemble_cycle_stats,
    reset_ensemble_cycle_stats,
)
from telegram import Bot, Update
from telegram.error import InvalidToken, NetworkError, TimedOut, TelegramError
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes

OPEN_METEO_ECMWF_URL = "https://api.open-meteo.com/v1/ecmwf"
OPEN_METEO_ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
OPEN_METEO_ENSEMBLE_MODEL = "ecmwf_ifs025"  # ECMWF IFS 0.25° Ensemble (51 members)
POLYMARKET_GAMMA_BASE = "https://gamma-api.polymarket.com"

# Переиспользуем соединения (быстрее, меньше зависаний)
HTTP = requests.Session()


def _http_get_json(url: str, *, params: dict | None = None, timeout: int = 30, retries: int = 3) -> dict | list:
    """
    Простая защита от 429 (rate limit): подождать и повторить.
    """
    last_err = None
    for attempt in range(retries):
        r = HTTP.get(url, params=params, timeout=timeout)
        if r.status_code == 429:
            # экспоненциальная пауза: 2s, 4s, 8s
            wait_s = 2 ** (attempt + 1)
            import time

            print(f"[Open‑Meteo] 429 rate limit, жду {wait_s}s и повторяю...", flush=True)
            time.sleep(wait_s)
            last_err = RuntimeError(f"429 rate limit for {url}")
            continue
        r.raise_for_status()
        return r.json()
    raise last_err or RuntimeError(f"HTTP failed for {url}")

# Итерации по рынкам — только города с mode != disabled (см. station_config.CityStationConfig.mode).
def _enabled_city_dicts() -> list[dict]:
    """Обратная совместимость: [{'name': 'London', 'lat': …, 'lon': …}] по координатам resolving station."""
    return [
        {
            "name": c.display_name_en,
            "lat": c.station_lat,
            "lon": c.station_lon,
        }
        for c in iter_enabled_city_configs()
    ]


CITY_SLUG_PREFIX = {
    "London": "highest-temperature-in-london-on-",
    "Paris": "highest-temperature-in-paris-on-",
    "Munich": "highest-temperature-in-munich-on-",
    "Milan": "highest-temperature-in-milan-on-",
    "Madrid": "highest-temperature-in-madrid-on-",
    "Warsaw": "highest-temperature-in-warsaw-on-",
    "Moscow": "highest-temperature-in-moscow-on-",
    "Ankara": "highest-temperature-in-ankara-on-",
    "Helsinki": "highest-temperature-in-helsinki-on-",
}

# Календарный день в slug события Polymarket = локальный день города (погода/резолв).
# Раньше брали UTC date → на границе суток D+1/D+2 указывали на соседний день и другой event slug.
CITY_TIMEZONE: dict[str, str] = {
    "London": "Europe/London",
    "Paris": "Europe/Paris",
    "Munich": "Europe/Berlin",
    "Milan": "Europe/Rome",
    "Madrid": "Europe/Madrid",
    "Warsaw": "Europe/Warsaw",
    "Moscow": "Europe/Moscow",
    "Ankara": "Europe/Istanbul",
    "Helsinki": "Europe/Helsinki",
}

def _local_today_for_city(city_name: str) -> dt.date:
    tz_name = CITY_TIMEZONE.get(city_name)
    assert tz_name, f"Нет таймзоны для города: {city_name}"
    return dt.datetime.now(ZoneInfo(tz_name)).date()

SIGNAL_RULES = {
    1: {"main_max": 0.50, "neighbor_max": 0.25, "edge_skip": True},
    2: {"main_max": 0.40, "neighbor_max": 0.20, "edge_skip": True},
    3: {"main_max": 0.30, "neighbor_max": 0.15, "edge_skip": True},
}

GAP_THRESHOLD = {
    1: 0.22,
    2: 0.25,
    3: 0.25,
}

# База для динамического stake (как у Лондона по глубинам); для городов с низкой ликвидностью — см. BASE_RISK_BY_CITY
BASE_RISK_DEFAULT = {
    1: 12,  # D+1
    2: 7,   # D+2
    3: 5,   # D+3
}

# Хельсинки: те же правила/пороги, но суммарная рекомендация по лестнице ~$2–5 (тонкие бакеты на Polymarket)
BASE_RISK_BY_CITY: dict[str, dict[int, int]] = {
    "Helsinki": {1: 4, 2: 3, 3: 2},
}


def _base_risk_for_city(city: str, depth: int) -> int:
    t = BASE_RISK_BY_CITY.get(city)
    if t is not None:
        return t[depth]
    return BASE_RISK_DEFAULT[depth]

POSITIONS_FILE = os.path.join(os.path.dirname(__file__), "positions.json")
STATS_FILE = os.path.join(os.path.dirname(__file__), "stats.json")
BOT_LOG_FILE = os.path.join(os.path.dirname(__file__), "bot.log")

log = logging.getLogger("rainmaker")


def setup_logging() -> None:
    """Файл bot.log с ротацией в полночь, дублирование в stderr."""
    if log.handlers:
        return
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = TimedRotatingFileHandler(
        BOT_LOG_FILE,
        when="midnight",
        interval=1,
        backupCount=14,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(sh)
    log.propagate = False


def _send_telegram_crash_sync(text: str) -> None:
    """Синхронно (requests), без asyncio — для уведомления при падении процесса."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        return
    try:
        r = HTTP.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text[:4000]},
            timeout=12,
        )
        r.raise_for_status()
    except Exception:
        pass


def _install_exception_hooks() -> None:
    """Необработанные исключения в главном потоке и в потоках → лог + Telegram."""

    def excepthook(exc_type, exc_value, exc_tb):
        if exc_type is KeyboardInterrupt:
            sys.__excepthook__(exc_type, exc_value, exc_tb)
            return
        log.critical("Необработанное исключение (главный поток)", exc_info=(exc_type, exc_value, exc_tb))
        tb = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        msg = f"🚨 Критическая ошибка бота (процесс падает):\n\n{type(exc_value).__name__}: {exc_value}\n\n{tb[-3500:]}"
        _send_telegram_crash_sync(msg)
        sys.__excepthook__(exc_type, exc_value, exc_tb)

    sys.excepthook = excepthook

    if hasattr(threading, "excepthook"):
        _default_thread_excepthook = threading.excepthook

        def _forward_thread_exception(args) -> None:
            if _default_thread_excepthook is not None:
                _default_thread_excepthook(args)
            elif hasattr(threading, "__excepthook__"):
                threading.__excepthook__(args)

        def thread_excepthook(args) -> None:
            if args.exc_type is SystemExit:
                _forward_thread_exception(args)
                return
            log.critical(
                "Необработанное исключение в потоке %s",
                args.thread.name,
                exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
            )
            tb = "".join(
                traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback)
            )
            et = args.exc_type
            en = args.exc_value
            head = f"{et.__name__}: {en}" if et is not None else str(en)
            msg = (
                f"🚨 Критическая ошибка в потоке «{args.thread.name}»:\n\n{head}\n\n{tb[-3500:]}"
            )
            _send_telegram_crash_sync(msg)
            _forward_thread_exception(args)

        threading.excepthook = thread_excepthook

# Время завершения последнего раунда проверки сигналов (UTC); для /status в МСК.
_last_round_finished_utc: dt.datetime | None = None

# Чтобы доп. прогоны ECMWF (00:30/06:30/12:30/18:30 UTC) не дублировались в одном слоте
_last_ecmwf_utc_slot_key: str | None = None

CITY_RU = {c.display_name_en: c.display_name_ru for c in CITY_STATIONS}

MONTH_RU = {
    1: "января",
    2: "февраля",
    3: "марта",
    4: "апреля",
    5: "мая",
    6: "июня",
    7: "июля",
    8: "августа",
    9: "сентября",
    10: "октября",
    11: "ноября",
    12: "декабря",
}


def _record_last_round_finished() -> None:
    global _last_round_finished_utc
    _last_round_finished_utc = dt.datetime.now(dt.timezone.utc)


def _status_message_msk() -> str:
    ts = _last_round_finished_utc or dt.datetime.now(dt.timezone.utc)
    msk = ts.astimezone(ZoneInfo("Europe/Moscow"))
    lines = [f"✅ Бот жив, последняя проверка в {msk.strftime('%H:%M')} МСК"]
    try:
        from paper_settings import paper_trading_enabled

        if paper_trading_enabled():
            lines.append("📎 Paper: команда /paper")
    except Exception:
        pass
    return "\n".join(lines)


async def on_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=_status_message_msk())


async def on_paper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat:
        return
    try:
        from paper_telegram_messages import format_paper_status_message

        await context.bot.send_message(chat_id=update.effective_chat.id, text=format_paper_status_message())
    except Exception:
        log.exception("on_paper")
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Paper status: ошибка (см. лог).")


RESTART_REPLY = "🔄 Отправляю сигнал на перезапуск. Если не заработает — зайди в Railway."


async def on_restart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=RESTART_REPLY)


async def send_startup_message() -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print(
            "Ошибка: не заданы переменные окружения TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID.",
            file=sys.stderr,
        )
        sys.exit(1)
    # Самая частая проблема у новичков — токен с пробелами или "не похожий" на токен.
    token = token.strip()
    if ":" not in token or len(token) < 20:
        print(
            "Ошибка: TELEGRAM_BOT_TOKEN выглядит неверно. Скопируй токен заново из BotFather (целиком, без пробелов).",
            file=sys.stderr,
        )
        sys.exit(1)
    bot = Bot(token=token)
    try:
        await bot.send_message(chat_id=chat_id, text="Бот запущен")
    except InvalidToken:
        print(
            "Ошибка Telegram: InvalidToken (токен неверный).\n"
            "Что сделать:\n"
            "- Открой @BotFather → выбери бота → API Token → Copy\n"
            "- В PowerShell задай переменную заново: $env:TELEGRAM_BOT_TOKEN=\"<токен>\"\n"
            "- Убедись, что нет лишних пробелов/кавычек и токен не обрезан.",
            file=sys.stderr,
        )
        sys.exit(1)
    except (TimedOut, NetworkError):
        print(
            "Ошибка Telegram: сеть/таймаут. Проверь интернет и попробуй ещё раз.",
            file=sys.stderr,
        )
        sys.exit(1)
    except TelegramError as e:
        print(f"Ошибка Telegram: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
    print("Сообщение «Бот запущен» отправлено в Telegram.")


def fetch_ecmwf_max_temperatures(lat: float, lon: float, forecast_days: int = 4) -> list[tuple[str, float]]:
    """
    Возвращает список из forecast_days элементов: (date_iso, tmax_c).

    Важно: Open‑Meteo часто включает "сегодня" как первый день.
    Чтобы уверенно покрыть D+1/D+2/D+3, по умолчанию берём 4 дня.
    """
    resp = requests.get(
        OPEN_METEO_ECMWF_URL,
        params={
            "latitude": lat,
            "longitude": lon,
            "daily": "temperature_2m_max",
            "forecast_days": forecast_days,
            "timezone": "UTC",
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    daily = data.get("daily") or {}
    dates = daily.get("time") or []
    tmax = daily.get("temperature_2m_max") or []
    n = min(len(dates), len(tmax), forecast_days)
    if n < 3:
        raise RuntimeError("Open-Meteo вернул недостаточно данных (нужно минимум 3 дня).")
    return list(zip(dates[:n], tmax[:n]))


def log_weather_once() -> None:
    print("=== ECMWF (Open-Meteo): max температура, 3 дня ===")
    now_utc = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"Время запроса: {now_utc}")
    for city in _enabled_city_dicts():
        temps_all = fetch_ecmwf_max_temperatures(city["lat"], city["lon"], forecast_days=4)
        # Берём D+1/D+2/D+3 как 2..4 элементы, если первый день = сегодня.
        # Если "сегодня" не пришёл, просто берём первые 3.
        temps = temps_all[1:4] if len(temps_all) >= 4 else temps_all[:3]
        pieces = []
        for i, (date_iso, tmax_c) in enumerate(temps, start=1):
            pieces.append(f"D+{i} {date_iso}: {tmax_c}°C")
        print(f'{city["name"]}: ' + " | ".join(pieces))
    print("=== конец ===")

def _date_to_event_slug_date(d: dt.date) -> str:
    """
    Polymarket URL: /event/highest-temperature-in-<city>-on-<month>-<day>-<year>
    Месяц — полное английское имя в нижнем регистре (как в реальных URL Gamma).
    День без ведущего нуля (april-9-2026, не april-09-2026 — иначе 404).
    """
    month = calendar.month_name[d.month].lower()
    return f"{month}-{d.day}-{d.year}"


def build_event_slug(city_name: str, d: dt.date) -> str:
    """
    d — календарная дата события в таймзоне города (день, для которого рынок «Highest temperature on …»).
    """
    prefix = CITY_SLUG_PREFIX.get(city_name)
    assert prefix, f"Ошибка: нет slug-префикса для города {city_name}"
    return prefix + _date_to_event_slug_date(d)


def check_event_exists(event_slug: str) -> bool:
    url = f"{POLYMARKET_GAMMA_BASE}/events/slug/{event_slug}"
    r = HTTP.get(url, timeout=30)
    if r.status_code == 404:
        return False
    r.raise_for_status()
    return True


def fetch_ensemble_tmax_members_for_date(
    city_name: str,
    target_date: dt.date,
    *,
    forecast_days: int = 4,
    past_days: int = 0,
) -> tuple[str, list[float]]:
    """
    Ensemble ECMWF (51 members) через Open‑Meteo.
    Возвращает (unit, members[51]) для temperature_2m_max на target_date.
    Координаты запроса — resolving station (station_config), не абстрактный центр города.
    Полный JSON ответа кэшируется по (город/station coords, forecast_days, past_days, model),
    чтобы D+1..D+3 в одном раунде не делали три одинаковых HTTP-запроса.
    """
    lat, lon = _get_city_coords(city_name)
    data = get_ensemble_daily_json_cached(
        city_name=city_name,
        lat=lat,
        lon=lon,
        forecast_days=forecast_days,
        past_days=past_days,
        model=OPEN_METEO_ENSEMBLE_MODEL,
        ensemble_url=OPEN_METEO_ENSEMBLE_URL,
        session=HTTP,
    )
    daily = data.get("daily") or {}
    times = daily.get("time") or []
    assert isinstance(times, list) and len(times) > 0, "Ensemble: daily.time пустой"
    try:
        idx = times.index(target_date.isoformat())
    except ValueError as e:
        raise AssertionError(
            f"Ensemble ECMWF не вернул дату {target_date.isoformat()} для {city_name}. Пришли даты: {times}"
        ) from e

    unit = ((data.get("daily_units") or {}).get("temperature_2m_max")) or "°C"
    # В ответе ECMWF ensemble сейчас присутствуют member01..member50 (50 шт).
    # Чтобы получить 51 значение как в ТЗ (51 член), добавляем "control" из temperature_2m_max.
    control_arr = daily.get("temperature_2m_max")
    assert isinstance(control_arr, list) and len(control_arr) > idx, "Ensemble: нет temperature_2m_max для control"
    control_val = control_arr[idx]
    assert control_val is not None, "Ensemble: control = None (нет данных на эту дату)"
    assert isinstance(control_val, (int, float)), f"Ensemble: control не число: {control_val}"

    members: list[float] = [float(control_val)]
    for i in range(1, 51):
        key = f"temperature_2m_max_member{i:02d}"
        arr = daily.get(key)
        assert isinstance(arr, list) and len(arr) > idx, f"Ensemble: нет данных {key} для idx={idx}"
        val = arr[idx]
        assert val is not None, f"Ensemble: {key}[{idx}] = None (нет данных на эту дату)"
        assert isinstance(val, (int, float)), f"Ensemble: {key}[{idx}] не число: {val}"
        members.append(float(val))
    assert len(members) == 51
    return unit, members


def load_positions() -> dict:
    try:
        with open(POSITIONS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def save_positions(data: dict) -> None:
    tmp = POSITIONS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, POSITIONS_FILE)


def load_stats() -> dict:
    try:
        with open(STATS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def save_stats(data: dict) -> None:
    tmp = STATS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATS_FILE)


def _parse_iso_utc(s: str) -> dt.datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return dt.datetime.fromisoformat(s)


def _signal_cooldown_hours() -> float:
    return float(os.environ.get("SIGNAL_DEDUP_COOLDOWN_HOURS", "6"))


def _count_active_positions() -> int:
    p = load_positions()
    n = 0
    for _slug, buckets in p.items():
        if isinstance(buckets, dict):
            n += len(buckets)
    return n


def _append_daily_signal_line(stats: dict, res: dict) -> None:
    """Накопление строки для ежедневного отчёта (календарный день по МСК)."""
    msk_today = dt.datetime.now(ZoneInfo("Europe/Moscow")).date().isoformat()
    dr = stats.setdefault("daily_report_by_msk", {})
    if not isinstance(dr, dict):
        dr = {}
        stats["daily_report_by_msk"] = dr
    lst = dr.setdefault(msk_today, [])
    lst.append(
        {
            "city_ru": CITY_RU.get(res["city"], res["city"]),
            "depth": int(res["depth"]),
            "bucket": _format_bucket_label(res["main"], res["unit"]),
            "price": float(res["main"]["yes"]),
            "gap_pct": int(round(float(res["gap"]) * 100)),
        }
    )


def _build_daily_report_message(for_day_msk: dt.date) -> str:
    stats = load_stats()
    dr = stats.get("daily_report_by_msk") or {}
    if not isinstance(dr, dict):
        dr = {}
    items = dr.get(for_day_msk.isoformat()) or []
    mo = MONTH_RU.get(for_day_msk.month, str(for_day_msk.month))
    lines = [
        f"📊 ОТЧЁТ ЗА {for_day_msk.day} {mo.upper()}",
        f"Сигналов отправлено: {len(items)}",
    ]
    for it in items:
        lines.append(
            f"• {it['city_ru']} D+{it['depth']}: {it['bucket']}, цена {it['price']:.2f}, разрыв {it['gap_pct']}%"
        )
    lines.append(f"Активных позиций: {_count_active_positions()}")
    next_d = for_day_msk + dt.timedelta(days=1)
    lines.append(
        f"Следующий отчёт: {next_d.day} {MONTH_RU[next_d.month]} {next_d.year} 00:00 МСК"
    )
    return "\n".join(lines)


def _maybe_send_daily_report_msk() -> None:
    """В 00:00 МСК — отчёт за вчерашний календарный день."""
    now_msk = dt.datetime.now(ZoneInfo("Europe/Moscow"))
    if now_msk.hour != 0 or now_msk.minute != 0:
        return
    report_day = now_msk.date() - dt.timedelta(days=1)
    key = report_day.isoformat()
    stats = load_stats()
    if stats.get("last_daily_report_sent_for") == key:
        return
    try:
        text = _build_daily_report_message(report_day)
        asyncio.run(send_telegram_text(text))
    except Exception:
        log.exception("не удалось отправить ежедневный отчёт")
        return
    try:
        from paper_settings import paper_trading_enabled

        if paper_trading_enabled():
            from paper_reports import format_daily_telegram_summary, format_weekly_telegram_summary, write_daily_report_file

            write_daily_report_file(for_day_msk=report_day)
            send_paper_telegram_safe(format_daily_telegram_summary(for_day_msk=report_day))
            now_msk2 = dt.datetime.now(ZoneInfo("Europe/Moscow"))
            if now_msk2.weekday() == 6:
                from paper_reports import write_weekly_report_file

                write_weekly_report_file(week_end_msk=report_day)
                send_paper_telegram_safe(format_weekly_telegram_summary(week_end_msk=report_day))
    except Exception:
        log.exception("paper daily/weekly report")
    stats = load_stats()
    stats["last_daily_report_sent_for"] = key
    dr = stats.setdefault("daily_report_by_msk", {})
    if isinstance(dr, dict):
        dr.pop(key, None)
    save_stats(stats)
    log.info("ежедневный отчёт отправлён за %s (МСК)", key)


def run_signals_round(*, respect_dedup: bool = True, ecmwf_bulletin_recheck: bool = False) -> int:
    """
    Один проход по всем городам и D+1..3: отправка Telegram при сигнале.
    respect_dedup: не слать повторно по тому же event_slug чаще, чем SIGNAL_DEDUP_COOLDOWN_HOURS
    (хранится в stats.json).
    ecmwf_bulletin_recheck: True при внеочередном слоте после ECMWF — paper engine помечает recheck.
    """
    reset_ensemble_cycle_stats()
    now = dt.datetime.now(dt.timezone.utc)
    cooldown = dt.timedelta(hours=_signal_cooldown_hours())
    stats = load_stats()
    sent_map: dict = stats.setdefault("sent_signals", {})
    sent = 0
    batch: list[tuple[str, dict, dt.datetime]] = []
    try:
        for city in _enabled_city_dicts():
            city_name = city["name"]
            local_today = _local_today_for_city(city_name)
            dates = [local_today + dt.timedelta(days=i) for i in (1, 2, 3)]
            for d in dates:
                event_slug = build_event_slug(city_name, d)
                if not check_event_exists(event_slug):
                    continue
                res = evaluate_signal_for_event(event_slug)
                batch.append((event_slug, res, now))

        # Сигналы в Telegram до paper phase, чтобы PAPER ENTRY мог reply на корневой сигнал в том же раунде.
        from telegram_signal_linkage import record_signal_message

        chat_id_for_link = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
        for event_slug, res, _ in batch:
            if res.get("skip"):
                continue
            if not res.get("signal"):
                continue
            if respect_dedup:
                rec = sent_map.get(event_slug)
                if isinstance(rec, dict) and rec.get("at"):
                    try:
                        last = _parse_iso_utc(str(rec["at"]))
                        if now - last < cooldown:
                            continue
                    except (ValueError, TypeError):
                        pass
            msg = build_signal_message(res)
            try:
                mid = asyncio.run(send_telegram_text(msg))
            except Exception:
                log.exception("telegram: не удалось отправить сигнал для %s", event_slug)
                continue
            sent += 1
            if mid is not None and chat_id_for_link:
                record_signal_message(event_slug, chat_id_for_link, mid, now)
            _append_daily_signal_line(stats, res)
            if respect_dedup:
                sent_map[event_slug] = {"at": now.replace(microsecond=0).isoformat()}
            save_stats(stats)

        try:
            from paper_engine import init_paper_if_missing, run_paper_phase

            init_paper_if_missing()
            run_paper_phase(batch, now=now, ecmwf_bulletin_recheck=ecmwf_bulletin_recheck)
        except Exception as e:
            log.exception("paper trading: %s", e)

        log.info("раунд проверки завершён, отправлено сигналов: %s", sent)
        return sent
    finally:
        log_ensemble_cycle_stats()
        _record_last_round_finished()


def _run_signals_round_safe(*, ecmwf_bulletin: bool = False) -> None:
    try:
        run_signals_round(respect_dedup=True, ecmwf_bulletin_recheck=ecmwf_bulletin)
    except Exception as e:
        log.exception("ошибка раунда проверки сигналов: %s", e)


def _maybe_ecmwf_utc_slot() -> None:
    """
    Дополнительные полные проверки в 00:30 / 06:30 / 12:30 / 18:30 UTC
    (после обновлений ECMWF в 00/06/12/18 UTC). Логика та же, что у основного расписания.
    """
    global _last_ecmwf_utc_slot_key
    now = dt.datetime.now(dt.timezone.utc)
    if now.minute != 30 or now.hour not in (0, 6, 12, 18):
        return
    slot_key = f"{now.date().isoformat()}T{now.hour:02d}:30"
    if _last_ecmwf_utc_slot_key == slot_key:
        return
    _last_ecmwf_utc_slot_key = slot_key
    log.info("доп. проверка по расписанию ECMWF (UTC %s)", slot_key)
    _run_signals_round_safe(ecmwf_bulletin=True)


def _start_periodic_signals() -> None:
    """Фоновый поток: schedule — основной интервал + фиксированные слоты UTC."""
    flag = os.environ.get("SIGNAL_PERIODIC", "1").strip().lower()
    if flag in ("0", "false", "no", "off"):
        log.info("Периодические сигналы отключены (SIGNAL_PERIODIC=0).")
        return
    interval_sec = int(os.environ.get("SIGNAL_INTERVAL_SEC", "1800"))
    first_delay = int(os.environ.get("SIGNAL_FIRST_DELAY_SEC", "60"))

    def loop() -> None:
        import time

        time.sleep(first_delay)
        _run_signals_round_safe()

        schedule.clear()
        if interval_sec >= 60 and interval_sec % 60 == 0:
            schedule.every(interval_sec // 60).minutes.do(_run_signals_round_safe)
        else:
            schedule.every(interval_sec).seconds.do(_run_signals_round_safe)

        schedule.every(15).seconds.do(_maybe_ecmwf_utc_slot)
        schedule.every(20).seconds.do(_maybe_send_daily_report_msk)

        while True:
            schedule.run_pending()
            time.sleep(1)

    threading.Thread(target=loop, name="rainmaker-signals", daemon=True).start()
    log.info(
        "режим реального времени: основная проверка каждые %ss (schedule); "
        "доп. слоты UTC 00:30/06:30/12:30/18:30; ежедневный отчёт 00:00 МСК; "
        "первая основная — через %ss; дедуп slug — %sч",
        interval_sec,
        first_delay,
        _signal_cooldown_hours(),
    )


def _inc(counter: dict, key: str, n: int = 1) -> None:
    counter[key] = int(counter.get(key, 0)) + n


def _apply_bucket_continuity(low: int | None, high: int | None) -> tuple[float | None, float | None]:
    """
    Polymarket бакеты по температуре дискретные (обычно по 1°).
    Чтобы вероятность не была ~0 для точного значения (low==high),
    используем "половинный шаг" (continuity correction):

    - 3°C -> [2.5, 3.5]
    - 57-58°F -> [56.5, 58.5]
    - <= 16°C -> (-inf, 16.5)
    - >= 67°F -> (66.5, +inf)
    """
    if low is not None and high is not None:
        lo = float(min(low, high)) - 0.5
        hi = float(max(low, high)) + 0.5
        return lo, hi
    if low is None and high is not None:
        return None, float(high) + 0.5
    if high is None and low is not None:
        return float(low) - 0.5, None
    return None, None


def _ensemble_members_in_bucket(member_vals: list[float], bucket: dict) -> int:
    """Сколько членов ансамбля попало в температурный бакет (как для p_main)."""
    prob_low, prob_high = _apply_bucket_continuity(bucket["low"], bucket["high"])
    cnt = 0
    for v in member_vals:
        if prob_low is None and prob_high is not None:
            ok = v <= prob_high
        elif prob_high is None and prob_low is not None:
            ok = v >= prob_low
        else:
            ok = (prob_low is not None) and (prob_high is not None) and (prob_low <= v <= prob_high)
        if ok:
            cnt += 1
    return cnt


def _gamma_market_stable_ids(m: dict) -> tuple[str, str]:
    """Стабильные id из Gamma для сопоставления ног при mark/update (не только question)."""
    mid = str(m.get("id") or "").strip()
    cid = str(m.get("conditionId") or m.get("condition_id") or "").strip()
    return mid, cid


def cmd_check_slugs() -> None:
    """
    Проверяет все города x 3 даты (D+1/D+2/D+3) и печатает какие события существуют в Gamma.
    """
    print("=== Проверка slug (города x 3 даты) ===")
    for city in _enabled_city_dicts():
        city_name = city["name"]
        local_today = _local_today_for_city(city_name)
        dates = [local_today + dt.timedelta(days=i) for i in (1, 2, 3)]
        for i, d in enumerate(dates, start=1):
            slug = build_event_slug(city_name, d)
            exists = check_event_exists(slug)
            print(f"{city_name} D+{i} {d.isoformat()} | {slug} | {'FOUND' if exists else '404'}")
    print("=== конец ===")


def test_openmeteo() -> list[float]:
    lat, lon = _get_city_coords("London")
    url = (
        f"https://api.open-meteo.com/v1/ecmwf"
        f"?latitude={lat}&longitude={lon}&daily=temperature_2m_max&forecast_days=3&timezone=UTC"
    )
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    temps = (data.get("daily") or {}).get("temperature_2m_max")
    if temps is None:
        raise AssertionError("Ошибка: temperature_2m_max отсутствует (None/null).")
    if not isinstance(temps, list):
        raise AssertionError(f"Ошибка: temperature_2m_max должен быть массивом, а не {type(temps)}")

    assert len(temps) == 3, f"Ошибка: ожидалось 3 значения, получено {len(temps)}"
    assert all(isinstance(t, (int, float)) for t in temps), f"Ошибка: не все значения числовые {temps}"

    print(f"[OK] Open-Meteo: прогноз (2-й элемент) = {temps[1]}°C")
    return [float(t) for t in temps]

def dump_openmeteo_example(city_name: str = "Paris") -> None:
    """
    Печатает ссылку на Open‑Meteo и выводит ключевые поля ответа,
    чтобы можно было глазами сверить то же самое в браузере и в PowerShell.
    """
    lat, lon = _get_city_coords(city_name)
    fd = _forecast_diag(city_name)
    url = (
        "https://api.open-meteo.com/v1/ecmwf"
        f"?latitude={lat}&longitude={lon}&daily=temperature_2m_max&forecast_days=3&timezone=UTC"
    )
    print("=== Open‑Meteo RAW ===")
    print(
        f"Город: {city_name} | station {fd['station_code']} ({fd['station_name']}) | "
        f"source_type={fd['source_type']} | forecast lat/lon={lat}, {lon} | mode={fd['city_mode']}"
    )
    print(f"Polymarket resolution URL: {fd['resolution_url']}")
    print("Открой в браузере и сравни с консолью:")
    print(url)
    r = HTTP.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    daily = data.get("daily") or {}
    print("daily.time:", daily.get("time"))
    print("daily.temperature_2m_max:", daily.get("temperature_2m_max"))
    print("=== конец ===")


def _gamma_get(path: str, params: dict | None = None) -> dict:
    url = f"{POLYMARKET_GAMMA_BASE}{path}"
    r = HTTP.get(url, params=params, timeout=30)
    if r.status_code == 404:
        raise AssertionError(f"Gamma API вернул 404 для {url}")
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        raise AssertionError(f"Gamma API вернул не dict, а {type(data)}")
    return data


def _extract_outcome_prices_field(obj: dict) -> list[float]:
    """
    В Gamma встречаются разные поля/форматы:
    - outcome_prices: строка JSON (как в ТЗ-дополнении)
    - outcomePrices: список строк/чисел
    - outcomePrices: строка JSON
    """
    raw = None
    if "outcome_prices" in obj:
        raw = obj.get("outcome_prices")
    elif "outcomePrices" in obj:
        raw = obj.get("outcomePrices")

    assert raw is not None, "Ошибка: outcome_prices/outcomePrices отсутствует или None."

    # Вариант A: строка, внутри JSON-массив
    if isinstance(raw, str):
        assert raw.strip() != "", "Ошибка: outcome_prices/outcomePrices пустая строка."
        parsed = json.loads(raw)
    else:
        parsed = raw

    # Вариант B: сразу массив
    assert isinstance(parsed, list), f"Ошибка: outcome prices после парсинга не list, а {type(parsed)}"
    assert len(parsed) > 0, "Ошибка: outcome prices пустой массив."

    try:
        prices_float = [float(p) for p in parsed]
    except Exception as e:
        raise AssertionError(f"Ошибка: не удалось конвертировать outcome prices в float: {parsed}") from e

    assert all(0.0 <= p <= 1.0 for p in prices_float), f"Ошибка: цены вне диапазона {prices_float}"
    return prices_float


def _extract_outcomes_field(obj: dict) -> list[str]:
    raw = None
    if "outcomes" in obj:
        raw = obj.get("outcomes")
    elif "outcomeNames" in obj:
        raw = obj.get("outcomeNames")
    assert raw is not None, "Ошибка: outcomes/outcomeNames отсутствует или None."
    if isinstance(raw, str):
        # Иногда Gamma отдает outcomes как строку JSON
        raw = json.loads(raw)
    assert isinstance(raw, list), f"Ошибка: outcomes должен быть list, а не {type(raw)}"
    assert len(raw) > 0, "Ошибка: outcomes пустой."
    assert all(isinstance(x, str) and x.strip() != "" for x in raw), f"Ошибка: outcomes содержит не строки: {raw}"
    return raw


def _to_f(x: object) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _yes_price_and_liquidity(m: dict) -> dict:
    """
    Цена YES для расчётов и отображения + ликвидность.

    Polymarket UI часто показывает стакан (best bid / best ask), а поле outcomePrices в Gamma —
    сглаженное/индикативное значение; lastTradePrice — последняя сделка. Поэтому для gap и
    «главной» цены по умолчанию берём mid = (bestBid + bestAsk) / 2, если оба есть
    (переменная POLYMARKET_USE_BOOK_MID=0 — только outcomePrices).
    """
    outcomes = _extract_outcomes_field(m)
    prices = _extract_outcome_prices_field(m)
    yes_idx = 0
    if outcomes[0].strip().lower() == "no" and outcomes[1].strip().lower() == "yes":
        yes_idx = 1
    outcome_yes = float(prices[yes_idx])
    bb = _to_f(m.get("bestBid"))
    ba = _to_f(m.get("bestAsk"))
    use_mid = os.environ.get("POLYMARKET_USE_BOOK_MID", "1").strip().lower() not in ("0", "false", "no", "off")
    if use_mid and bb is not None and ba is not None:
        yes = (bb + ba) / 2.0
    else:
        yes = outcome_yes
    vol_raw = m.get("volumeNum")
    if vol_raw is None:
        vol_raw = m.get("volumeClob")
    if vol_raw is None:
        try:
            vol_raw = float(m.get("volume") or 0)
        except (TypeError, ValueError):
            vol_raw = 0.0
    volume = float(vol_raw)
    return {
        "yes": yes,
        "outcome_yes": outcome_yes,
        "volume": volume,
        "best_bid": bb,
        "best_ask": ba,
        "last_trade": _to_f(m.get("lastTradePrice")),
    }


def _format_bucket_telegram_line(label: str, b: dict, stake_amt: int) -> str:
    """Одна строка лестницы: ставка, mid, опционально bid/ask и объём."""
    mid = float(b["yes"])
    parts = [f"• {label}: ${stake_amt} (mid {mid:.2f})"]
    bb, ba = b.get("best_bid"), b.get("best_ask")
    if bb is not None and ba is not None:
        parts.append(f"bid {bb:.2f} / ask {ba:.2f}")
    vol = float(b.get("volume") or 0)
    if vol > 0:
        parts.append(f"vol ${vol:,.0f}".replace(",", " "))
    return " · ".join(parts)


def _gamma_get_market_by_slug(slug: str) -> dict:
    """
    Slug на сайте Polymarket — это slug EVENT (страница /event/<slug>).
    В Gamma его корректнее дергать через /events/slug/<slug>, а уже внутри брать market.
    Но иногда slug может совпадать и с market slug — пробуем оба варианта.
    """
    # 1) Попытка как market slug
    try:
        return _gamma_get(f"/markets/slug/{slug}")
    except AssertionError:
        pass

    # 2) Попытка как event slug
    event = _gamma_get(f"/events/slug/{slug}")
    markets = event.get("markets")
    assert isinstance(markets, list) and len(markets) > 0, "Ошибка: в event нет markets или он пустой."
    first = markets[0]
    assert isinstance(first, dict), "Ошибка: event.markets[0] не dict."
    return first


def _gamma_get_event_by_slug(event_slug: str) -> dict:
    event = _gamma_get(f"/events/slug/{event_slug}")
    assert isinstance(event, dict), "Ошибка: event не dict."
    markets = event.get("markets")
    assert isinstance(markets, list) and len(markets) > 0, "Ошибка: в event нет markets или он пустой."
    return event


def _c_to_f(c: float) -> float:
    return (c * 9.0 / 5.0) + 32.0


def _detect_unit_from_text(text: str) -> str:
    # Возвращает 'C' или 'F' (по умолчанию 'C', если не видно)
    t = text.lower()
    if "°f" in t or " f" in t:
        return "F"
    if "°c" in t or " c" in t:
        return "C"
    return "C"


def _parse_event_slug_date(event_slug: str) -> dt.date:
    # ...-on-april-10-2026
    m = re.search(r"-on-([a-z]+)-(\d{1,2})-(\d{4})$", event_slug)
    assert m, f"Не смог распарсить дату из slug: {event_slug}"
    month_name, day_s, year_s = m.group(1), m.group(2), m.group(3)
    month_name = month_name.lower()
    month_map = {calendar.month_name[i].lower(): i for i in range(1, 13)}
    assert month_name in month_map, f"Неизвестный месяц в slug: {month_name}"
    return dt.date(int(year_s), month_map[month_name], int(day_s))


def _parse_city_from_event_slug(event_slug: str) -> str:
    # highest-temperature-in-london-on-...
    m = re.search(r"highest-temperature-in-([a-z]+)-on-", event_slug)
    assert m, f"Не смог распарсить город из slug: {event_slug}"
    return city_config_from_slug_token(m.group(1)).display_name_en


def _get_city_coords(city_name: str) -> tuple[float, float]:
    """Координаты для Open‑Meteo: resolving station (см. station_config), не абстрактный центр города."""
    c = city_config_by_display_name(city_name)
    return float(c.station_lat), float(c.station_lon)


def _forecast_diag(city_name: str) -> dict:
    """Поля для логов/сигналов: станция резолва Polymarket и точка прогноза."""
    c = city_config_by_display_name(city_name)
    return {
        "city_key": c.city_key,
        "station_code": c.station_code,
        "station_name": c.station_name,
        "source_type": c.source_type,
        "forecast_lat": c.station_lat,
        "forecast_lon": c.station_lon,
        "resolution_url": c.resolution_url,
        "city_mode": c.mode,
    }


def _analytical_and_entry_from_main_row(main_row: dict) -> tuple[float, float, float | None, float | None, float | None]:
    """
    analytical_price = mid (как в _yes_price_and_liquidity: yes).
    entry_price_assumed = bestAsk если есть, иначе analytical.
    Возвращает: analytical, entry_assumed, spread, best_bid, best_ask.
    """
    analytical = float(main_row["yes"])
    bb = _to_f(main_row.get("best_bid"))
    ba = _to_f(main_row.get("best_ask"))
    entry = float(ba) if ba is not None else analytical
    spread = (ba - bb) if (bb is not None and ba is not None) else None
    return analytical, entry, spread, bb, ba


def _liquidity_check_main_and_neighbors(
    *,
    depth: int,
    spread_main: float | None,
    volume_main: float,
    best_ask_main: float | None,
    volume_lower: float | None,
    volume_upper: float | None,
) -> list[str]:
    """Список кодов отказа; пусто = прошли фильтр."""
    reasons: list[str] = []
    if require_best_ask_for_entry() and best_ask_main is None:
        reasons.append("liquidity_no_best_ask")
    max_sp = liquidity_max_spread_main(depth)
    if spread_main is None:
        reasons.append("liquidity_spread_unknown")
    elif spread_main > max_sp:
        reasons.append("liquidity_spread_wide")
    min_vm = liquidity_min_volume_main(depth)
    if volume_main < min_vm:
        reasons.append("liquidity_volume_main_low")
    min_nb = liquidity_min_neighbor_volume(depth)
    if min_nb > 0:
        if volume_lower is not None and volume_lower < min_nb:
            reasons.append("liquidity_volume_neighbor_low")
        if volume_upper is not None and volume_upper < min_nb:
            reasons.append("liquidity_volume_neighbor_low")
    return reasons


def _journal_record_from_result(res: dict) -> None:
    """Пишет строку в signal_journal.jsonl; не бросает наружу."""
    slug = res.get("event_slug")
    if not slug:
        return
    try:
        city_en = res.get("city") or ""
        if not city_en:
            city_en = _parse_city_from_event_slug(slug)
        cfg = city_config_by_display_name(city_en)
        event_date = res.get("date") or ""
        if not event_date:
            try:
                event_date = _parse_event_slug_date(slug).isoformat()
            except Exception:
                event_date = ""

        main = res.get("main")
        low_b = res.get("low")
        high_b = res.get("high")
        unit = res.get("unit") or "C"

        def lbl(bucket: dict | None) -> str | None:
            if not bucket:
                return None
            return _format_bucket_label(bucket, unit)

        skip = bool(res.get("skip"))
        sig = bool(res.get("signal")) if not skip else False
        if skip:
            stype = "no_trade"
        else:
            stype = str(res.get("structure_type") or "no_trade")

        if skip:
            rej = [str(res.get("reason", "skip"))]
        else:
            rej = [x for x in (res.get("reasons") or []) if x != "ok"]

        stake = res.get("stake")
        low_amt = main_amt = high_amt = None
        la = res.get("ladder_allocation")
        if isinstance(la, dict):
            low_amt = la.get("low_usd")
            main_amt = la.get("main_usd")
            high_amt = la.get("high_usd")

        vctx = build_resolution_context(
            city_key=cfg.city_key,
            station_code=cfg.station_code,
            source_type=cfg.source_type,
            resolution_url=cfg.resolution_url,
            event_date=event_date,
            event_slug=slug,
            display_name_en=cfg.display_name_en,
            station_lat=cfg.station_lat,
            station_lon=cfg.station_lon,
            extra={"journal_version": 1},
        )

        record = {
            "timestamp_utc": utc_now_iso(),
            "timestamp_msk": msk_now_iso(),
            "city_key": cfg.city_key,
            "display_name_en": cfg.display_name_en,
            "display_name_ru": cfg.display_name_ru,
            "mode": cfg.mode,
            "source_type": cfg.source_type,
            "station_code": cfg.station_code,
            "station_name": cfg.station_name,
            "station_lat": cfg.station_lat,
            "station_lon": cfg.station_lon,
            "event_slug": slug,
            "event_url": f"https://polymarket.com/event/{slug}",
            "event_date": event_date,
            "depth": res.get("depth"),
            "main_bucket_label": lbl(main) if isinstance(main, dict) else None,
            "lower_bucket_label": lbl(low_b) if isinstance(low_b, dict) else None,
            "upper_bucket_label": lbl(high_b) if isinstance(high_b, dict) else None,
            "p_main": res.get("p_main"),
            "analytical_price": res.get("analytical_price"),
            "entry_price_assumed": res.get("entry_price_assumed"),
            "best_bid_main": res.get("best_bid_main"),
            "best_ask_main": res.get("best_ask_main"),
            "spread_main": res.get("spread_main"),
            "volume_main": res.get("volume_main"),
            "volume_neighbor_lower": res.get("volume_neighbor_lower"),
            "volume_neighbor_upper": res.get("volume_neighbor_upper"),
            "gap_analytical": res.get("gap_analytical"),
            "gap_entry": res.get("gap_entry"),
            "signal_passed": sig,
            "structure_type": stype,
            "rejection_reasons": rej,
            "stake_recommendation_total": stake,
            "ladder_allocation_usd": {
                "low": low_amt,
                "main": main_amt,
                "high": high_amt,
            },
            "resolution_url": cfg.resolution_url,
            "resolution_context": vctx,
            "skipped": skip,
        }
        append_signal_journal_record(record, path=signal_journal_path())
    except Exception as e:
        log.warning("journal build/skip: %s", e, exc_info=False)


def fetch_ecmwf_tmax_for_date(city_name: str, target_date: dt.date) -> float:
    lat, lon = _get_city_coords(city_name)
    daily = fetch_ecmwf_max_temperatures(lat, lon, forecast_days=4)
    for date_iso, tmax_c in daily:
        if date_iso == target_date.isoformat():
            assert isinstance(tmax_c, (int, float)), f"tmax не число: {tmax_c}"
            return float(tmax_c)
    raise AssertionError(
        f"ECMWF не вернул дату {target_date.isoformat()} для {city_name}. "
        f"Пришли даты: {[d for d,_ in daily]}"
    )


def _extract_range_from_question(question: str) -> tuple[int | None, int | None, str]:
    """
    Возвращает (low, high, unit) где unit = 'C' или 'F'.
    low/high могут быть None для 'or below'/'or higher'.
    Примеры:
    - '56°F or below' -> (None, 56, 'F')
    - 'between 57-58°F' -> (57, 58, 'F')
    - '67°F or higher' -> (67, None, 'F')
    - '16°C or below' -> (None, 16, 'C')
    """
    q = question.lower()
    unit = _detect_unit_from_text(question)

    # Вопросы Polymarket обычно устроены так:
    # "Will the highest temperature in Moscow be 3°C on April 9?"
    # "Will the highest temperature in London be between 57-58°F on October 13?"
    # "Will the highest temperature in Moscow be -2°C or below on April 9?"
    #
    # Важно: в строке есть и температура, и число дня месяца. Нам надо вытащить именно температуру.

    # 1) or below / or higher (может быть отрицательной)
    m = re.search(r"be\s+(-?\d{1,3})\s*°\s*(c|f)\s+or\s+below", q)
    if m:
        return None, int(m.group(1)), m.group(2).upper()
    m = re.search(r"be\s+(-?\d{1,3})\s*°\s*(c|f)\s+or\s+higher", q)
    if m:
        return int(m.group(1)), None, m.group(2).upper()

    # 2) between X-Y°F/°C
    m = re.search(r"between\s+(-?\d{1,3})\s*-\s*(-?\d{1,3})\s*°?\s*(c|f)", q)
    if m:
        return int(m.group(1)), int(m.group(2)), m.group(3).upper()

    # 3) точное значение "be 3°C on ..."
    m = re.search(r"be\s+(-?\d{1,3})\s*°\s*(c|f)\b", q)
    if m:
        v = int(m.group(1))
        return v, v, m.group(2).upper()

    raise AssertionError(f"Не смог распарсить диапазон температуры из вопроса: {question}")


def analyze_event_vs_ecmwf(event_slug: str) -> None:
    """
    Берём дату+город из event_slug, получаем ECMWF tmax и сопоставляем с лестницей.
    Печатаем: прогноз, выбранный диапазон, соседей, цены YES.
    """
    city = _parse_city_from_event_slug(event_slug)
    target_date = _parse_event_slug_date(event_slug)
    tmax_c = fetch_ecmwf_tmax_for_date(city, target_date)

    event = _gamma_get_event_by_slug(event_slug)
    markets = event["markets"]

    ladder: list[dict] = []
    ladder_unit: str | None = None
    for m in markets:
        if not isinstance(m, dict):
            continue
        question = m.get("question") or m.get("title") or ""
        if not isinstance(question, str) or not question.strip():
            continue
        outcomes = _extract_outcomes_field(m)
        prices = _extract_outcome_prices_field(m)
        if len(outcomes) != 2 or len(prices) != 2:
            continue
        low, high, unit = _extract_range_from_question(question)
        if ladder_unit is None:
            ladder_unit = unit
        if ladder_unit != unit:
            # Смешанные единицы в одном событии — лучше не рисковать
            continue
        liq = _yes_price_and_liquidity(m)
        gm_id, cond_id = _gamma_market_stable_ids(m)
        ladder.append(
            {
                "question": question,
                "low": low,
                "high": high,
                "gamma_market_id": gm_id,
                "condition_id": cond_id,
                **liq,
            }
        )

    assert ladder, "Не собрал лестницу (нет бинарных рынков Yes/No)."
    assert ladder_unit is not None

    forecast_val = tmax_c if ladder_unit == "C" else _c_to_f(tmax_c)

    # сортируем по нижней границе (None считаем -inf)
    def key(x: dict) -> int:
        return -10_000 if x["low"] is None else int(x["low"])

    ladder.sort(key=key)

    # Находим "главный" диапазон: куда попадает tmax_f
    def contains(x: dict) -> bool:
        low = x["low"]
        high = x["high"]
        if low is None and high is not None:
            return forecast_val <= high
        if high is None and low is not None:
            return forecast_val >= low
        if low is not None and high is not None:
            return low <= forecast_val <= high
        return False

    main_idx = None
    for i, x in enumerate(ladder):
        if contains(x):
            main_idx = i
            break
    if main_idx is None:
        # fallback: ближе всего по центру диапазона
        def dist(x: dict) -> float:
            low, high = x["low"], x["high"]
            if low is None and high is not None:
                center = float(high)
            elif high is None and low is not None:
                center = float(low)
            else:
                center = (float(low) + float(high)) / 2.0
            return abs(center - forecast_val)

        main_idx = min(range(len(ladder)), key=lambda i: dist(ladder[i]))

    fd = _forecast_diag(city)
    print("=== ANALYZE event vs ECMWF ===")
    print(f"event_slug: {event_slug}")
    print(f"city: {city} | date: {target_date.isoformat()}")
    print(
        f"station: {fd['station_code']} ({fd['station_name']}) | source={fd['source_type']} | "
        f"forecast lat/lon: {fd['forecast_lat']:.4f}, {fd['forecast_lon']:.4f} | mode={fd['city_mode']}"
    )
    print(f"resolution_url: {fd['resolution_url']}")
    if ladder_unit == "C":
        print(f"ECMWF tmax: {tmax_c:.1f}°C (сопоставляем в °C)")
    else:
        print(f"ECMWF tmax: {tmax_c:.1f}°C = {_c_to_f(tmax_c):.1f}°F (сопоставляем в °F)")

    def fmt_rng(x: dict) -> str:
        low, high = x["low"], x["high"]
        if low is None:
            return f"<= {high}°{ladder_unit}"
        if high is None:
            return f">= {low}°{ladder_unit}"
        return f"{low}-{high}°{ladder_unit}"

    main = ladder[main_idx]
    print(f"MAIN: {fmt_rng(main)} | YES={main['yes']} | {main['question']}")
    if main_idx > 0:
        lo = ladder[main_idx - 1]
        print(f"LOW : {fmt_rng(lo)} | YES={lo['yes']} | {lo['question']}")
    if main_idx + 1 < len(ladder):
        hi = ladder[main_idx + 1]
        print(f"HIGH: {fmt_rng(hi)} | YES={hi['yes']} | {hi['question']}")
    print("=== конец ===")


def evaluate_signal_for_event(event_slug: str, *, run_date: dt.date | None = None) -> dict:
    """
    Этап 4: расчёт "Сигнал/Нет сигнала" для одного события.
    run_date: только для бэктеста — якорная «сегодняшняя» дата; в бою не передаём,
    тогда «сегодня» = календарный день города события (таймзона из CITY_TIMEZONE).
    Прогноз ensemble — по координатам resolving station (station_config), не «центр города».

    Этап 2: analytical vs entry (best ask), gap_analytical / gap_entry, ликвидность, журнал.
    """
    city = _parse_city_from_event_slug(event_slug)
    fd = _forecast_diag(city)
    if fd["city_mode"] == "disabled":
        try:
            ed = _parse_event_slug_date(event_slug).isoformat()
        except Exception:
            ed = ""
        out = {"event_slug": event_slug, "skip": True, "reason": "city_disabled", "city": city, "date": ed, **fd}
        _journal_record_from_result(out)
        return out
    event_date = _parse_event_slug_date(event_slug)
    if run_date is not None:
        today = run_date
    else:
        today = _local_today_for_city(city)
    depth = (event_date - today).days
    if depth not in (1, 2, 3):
        out = {
            "event_slug": event_slug,
            "skip": True,
            "reason": f"depth={depth} (нужно 1..3)",
            "city": city,
            "date": event_date.isoformat(),
            **fd,
        }
        _journal_record_from_result(out)
        return out

    rules = SIGNAL_RULES[depth]
    gap_threshold = GAP_THRESHOLD[depth]
    base_risk = _base_risk_for_city(city, depth)
    # Ensemble ECMWF members (51)
    # При backtest/анализе можем использовать past_days, чтобы target_date точно попал в окно.
    past_days = 7 if (run_date is not None and event_date < dt.datetime.now(dt.timezone.utc).date()) else 0
    try:
        unit, members = fetch_ensemble_tmax_members_for_date(
            city, event_date, forecast_days=7, past_days=past_days
        )
    except OpenMeteoEnsembleUnavailable as e:
        out = {
            "event_slug": event_slug,
            "skip": True,
            "reason": "openmeteo_ensemble_unavailable",
            "openmeteo_rate_limited": bool(getattr(e, "rate_limited", False)),
            "city": city,
            "date": event_date.isoformat(),
            **fd,
        }
        log.warning(
            "ensemble недоступен для %s %s: %s (rate_limited=%s)",
            city,
            event_date.isoformat(),
            e,
            out["openmeteo_rate_limited"],
        )
        _journal_record_from_result(out)
        return out

    event = _gamma_get_event_by_slug(event_slug)
    markets = event["markets"]

    ladder: list[dict] = []
    ladder_unit: str | None = None
    for m in markets:
        if not isinstance(m, dict):
            continue
        question = m.get("question") or m.get("title") or ""
        if not isinstance(question, str) or not question.strip():
            continue
        outcomes = _extract_outcomes_field(m)
        prices = _extract_outcome_prices_field(m)
        if len(outcomes) != 2 or len(prices) != 2:
            continue
        low, high, unit = _extract_range_from_question(question)
        if ladder_unit is None:
            ladder_unit = unit
        if ladder_unit != unit:
            continue
        liq = _yes_price_and_liquidity(m)
        gm_id, cond_id = _gamma_market_stable_ids(m)
        ladder.append(
            {
                "question": question,
                "low": low,
                "high": high,
                "gamma_market_id": gm_id,
                "condition_id": cond_id,
                **liq,
            }
        )

    if not ladder:
        out = {
            "event_slug": event_slug,
            "skip": True,
            "reason": "no ladder",
            "city": city,
            "date": event_date.isoformat(),
            **fd,
        }
        _journal_record_from_result(out)
        return out
    assert ladder_unit is not None

    # В Polymarket рынок может быть в °C или °F. Open‑Meteo ensemble отдаёт в °C по умолчанию.
    # Если рынок в °F, конвертируем каждый member в °F.
    if ladder_unit == "C":
        member_vals = members
    else:
        member_vals = [_c_to_f(x) for x in members]

    # Для выбора "главного бакета" используем control member (первый элемент).
    forecast = member_vals[0]

    def key(x: dict) -> int:
        return -10_000 if x["low"] is None else int(x["low"])

    ladder.sort(key=key)

    def contains(x: dict) -> bool:
        low = x["low"]
        high = x["high"]
        if low is None and high is not None:
            return forecast <= high
        if high is None and low is not None:
            return forecast >= low
        if low is not None and high is not None:
            return low <= forecast <= high
        return False

    main_idx = None
    for i, x in enumerate(ladder):
        if contains(x):
            main_idx = i
            break
    if main_idx is None:
        def dist(x: dict) -> float:
            low, high = x["low"], x["high"]
            if low is None and high is not None:
                center = float(high)
            elif high is None and low is not None:
                center = float(low)
            else:
                center = (float(low) + float(high)) / 2.0
            return abs(center - forecast)

        main_idx = min(range(len(ladder)), key=lambda i: dist(ladder[i]))

    if rules["edge_skip"] and (main_idx == 0 or main_idx == len(ladder) - 1):
        out = {
            "event_slug": event_slug,
            "skip": True,
            "reason": "edge_bucket",
            "city": city,
            "date": event_date.isoformat(),
            **fd,
        }
        _journal_record_from_result(out)
        return out

    main = ladder[main_idx]
    low_nb = ladder[main_idx - 1] if main_idx > 0 else None
    high_nb = ladder[main_idx + 1] if main_idx + 1 < len(ladder) else None

    analytical_price, entry_price_assumed, spread_main, best_bid_main, best_ask_main = (
        _analytical_and_entry_from_main_row(main)
    )
    volume_main = float(main.get("volume") or 0)
    volume_neighbor_lower = float(low_nb["volume"]) if low_nb is not None else None
    volume_neighbor_upper = float(high_nb["volume"]) if high_nb is not None else None

    prob_low, prob_high = _apply_bucket_continuity(main["low"], main["high"])
    # Вероятность = доля членов ансамбля, попавших в бакет
    in_bucket = 0
    for v in member_vals:
        if prob_low is None and prob_high is not None:
            ok = v <= prob_high
        elif prob_high is None and prob_low is not None:
            ok = v >= prob_low
        else:
            ok = (prob_low is not None) and (prob_high is not None) and (prob_low <= v <= prob_high)
        if ok:
            in_bucket += 1
    p_main = in_bucket / 51.0
    p_lower = _ensemble_members_in_bucket(member_vals, low_nb) / 51.0 if low_nb is not None else None
    p_upper = _ensemble_members_in_bucket(member_vals, high_nb) / 51.0 if high_nb is not None else None
    gap_analytical = p_main - analytical_price
    gap_entry = p_main - entry_price_assumed
    gap = gap_analytical  # legacy: те же пороги GAP_THRESHOLD, что и раньше (от mid)

    neighbors = [x for x in (low_nb, high_nb) if x is not None]
    max_neighbor_price = max((x["yes"] for x in neighbors), default=0.0)

    ok_prices = (main["yes"] <= rules["main_max"]) and (max_neighbor_price <= rules["neighbor_max"])
    ok_gap = gap > gap_threshold

    liquidity_reasons = _liquidity_check_main_and_neighbors(
        depth=depth,
        spread_main=spread_main,
        volume_main=volume_main,
        best_ask_main=best_ask_main,
        volume_lower=volume_neighbor_lower,
        volume_upper=volume_neighbor_upper,
    )
    ok_liquidity = len(liquidity_reasons) == 0

    # Дополнительные правила уверенности (по твоему сообщению)
    # 1) Если p < 30% и цена главного > 0.02 -> не сигналить
    low_prob_block = (p_main < 0.30) and (main["yes"] > 0.02)
    # 2) Если 30-50% -> сигнал только если разрыв > 25%
    mid_prob_requires_gap25 = (0.30 <= p_main <= 0.50)
    mid_gap_ok = (gap > 0.25) if mid_prob_requires_gap25 else True
    # 3) Если > 60% -> пометка "высокая уверенность" (фильтрации не добавляет)
    high_confidence = p_main > 0.60

    ok_confidence = (not low_prob_block) and mid_gap_ok

    # Динамический размер ставки (пока от gap_analytical, как раньше)
    raw_stake = base_risk * (gap / gap_threshold) if gap_threshold > 0 else base_risk
    stake = int(round(raw_stake))
    stake = max(0, min(stake, base_risk * 2))
    if city == "Helsinki" and stake > 0:
        stake = max(2, min(stake, 5))

    low_amt, main_amt, high_amt = _split_stake_70_15_15(stake)
    ladder_allocation = {"low_usd": low_amt, "main_usd": main_amt, "high_usd": high_amt}

    positions = load_positions()
    bucket_key = f"{main.get('low')}..{main.get('high')}|{ladder_unit}"
    pos = positions.get(event_slug, {}).get(bucket_key) if isinstance(positions.get(event_slug), dict) else None
    stop_loss = False
    entry_price = None
    if isinstance(pos, dict) and isinstance(pos.get("entry_price"), (int, float)):
        entry_price = float(pos["entry_price"])
        stop_loss = main["yes"] < entry_price * 0.90

    reasons: list[str] = []
    if not ok_prices:
        reasons.append("prices")
    if not ok_gap:
        reasons.append("gap_threshold")
    if low_prob_block:
        reasons.append("low_prob_block")
    if (0.30 <= p_main <= 0.50) and (gap <= 0.25):
        reasons.append("mid_prob_gap25")
    reasons.extend(liquidity_reasons)
    if not reasons:
        reasons.append("ok")

    signal = bool(ok_prices and ok_gap and ok_confidence and ok_liquidity)

    if not signal:
        structure_type = "no_trade"
    elif low_nb is not None and high_nb is not None:
        structure_type = "ladder_3"
    else:
        structure_type = "single_bucket"

    cfg = city_config_by_display_name(city)
    resolution_context = build_resolution_context(
        city_key=cfg.city_key,
        station_code=cfg.station_code,
        source_type=cfg.source_type,
        resolution_url=cfg.resolution_url,
        event_date=event_date.isoformat(),
        event_slug=event_slug,
        display_name_en=cfg.display_name_en,
        station_lat=cfg.station_lat,
        station_lon=cfg.station_lon,
        extra={"gap_entry": gap_entry, "gap_analytical": gap_analytical},
    )

    out = {
        "event_slug": event_slug,
        "city": city,
        "date": event_date.isoformat(),
        "depth": depth,
        "unit": ladder_unit,
        "ensemble_unit": unit,
        "ensemble_members": 51,
        "p_main_members": in_bucket,
        "p_lower": p_lower,
        "p_upper": p_upper,
        "gap_threshold": gap_threshold,
        "base_risk": base_risk,
        "stake": stake,
        "entry_price": entry_price,
        "stop_loss": stop_loss,
        "main": main,
        "low": low_nb,
        "high": high_nb,
        "p_main": p_main,
        "analytical_price": analytical_price,
        "entry_price_assumed": entry_price_assumed,
        "best_bid_main": best_bid_main,
        "best_ask_main": best_ask_main,
        "spread_main": spread_main,
        "volume_main": volume_main,
        "volume_neighbor_lower": volume_neighbor_lower,
        "volume_neighbor_upper": volume_neighbor_upper,
        "gap_analytical": gap_analytical,
        "gap_entry": gap_entry,
        "gap": gap,
        "ok_prices": ok_prices,
        "ok_gap": ok_gap,
        "ok_liquidity": ok_liquidity,
        "ok_confidence": ok_confidence,
        "high_confidence": high_confidence,
        "signal": signal,
        "reasons": reasons,
        "rules": rules,
        "structure_type": structure_type,
        "ladder_allocation": ladder_allocation,
        "resolution_context": resolution_context,
        **fd,
    }
    _journal_record_from_result(out)
    return out


def cmd_signal_check() -> None:
    """
    Этап 4 тест: пробегаемся по всем городам x 3 даты и печатаем "Сигнал/Нет сигнала".
    """
    print("=== SIGNAL CHECK (этап 4) ===")
    print("(ECMWF ensemble по координатам resolving station из station_config.py)")
    for city in _enabled_city_dicts():
        city_name = city["name"]
        local_today = _local_today_for_city(city_name)
        dates = [local_today + dt.timedelta(days=i) for i in (1, 2, 3)]
        for i, d in enumerate(dates, start=1):
            event_slug = build_event_slug(city_name, d)
            if not check_event_exists(event_slug):
                print(f"{city_name} D+{i} {d.isoformat()} | 404")
                continue
            res = evaluate_signal_for_event(event_slug)
            if res.get("skip"):
                print(
                    f"{city_name} D+{i} {d.isoformat()} | SKIP | {res.get('reason')} | "
                    f"st={res.get('station_code')} src={res.get('source_type')}"
                )
                continue
            if res["signal"]:
                ge = res.get("gap_entry")
                ge_s = f"{ge:+.2f}" if isinstance(ge, (int, float)) else "?"
                print(
                    f"{city_name} D+{i} {d.isoformat()} | SIGNAL | "
                    f"p={res['p_main']:.2f}({res['p_main_members']}/51) mkt={res['main']['yes']:.2f} "
                    f"gap_ana={res['gap']:+.2f}>thr{res['gap_threshold']:.2f} stake=${res['stake']} "
                    f"(main<={res['rules']['main_max']}, nb<={res['rules']['neighbor_max']}) "
                    f"struct={res.get('structure_type')} gap_entry={ge_s} spread={res.get('spread_main')} "
                    f"| st={res.get('station_code')} src={res.get('source_type')}"
                )
            else:
                ge = res.get("gap_entry")
                ge_s = f"{ge:+.2f}" if isinstance(ge, (int, float)) else "?"
                print(
                    f"{city_name} D+{i} {d.isoformat()} | NO | "
                    f"p={res['p_main']:.2f}({res['p_main_members']}/51) mkt={res['main']['yes']:.2f} "
                    f"gap_ana={res['gap']:+.2f} thr={res['gap_threshold']:.2f} stake=${res['stake']} "
                    f"prices_ok={res['ok_prices']} gap_ok={res['ok_gap']} liq_ok={res.get('ok_liquidity')} "
                    f"conf_ok={res.get('ok_confidence')} struct={res.get('structure_type')} gap_entry={ge_s} "
                    f"| st={res.get('station_code')} src={res.get('source_type')}"
                )
    print("=== конец ===")


def cmd_debug() -> None:
    """
    --debug: печатает ВСЕ проверки по всем событиям (даже без сигнала).
    """
    print("=== DEBUG (все проверки) ===")
    for city in _enabled_city_dicts():
        city_name = city["name"]
        local_today = _local_today_for_city(city_name)
        dates = [local_today + dt.timedelta(days=i) for i in (1, 2, 3)]
        for depth, d in enumerate(dates, start=1):
            event_slug = build_event_slug(city_name, d)
            exists = check_event_exists(event_slug)
            print(
                f"\n[{city_name} D+{depth} {d.isoformat()}] slug={event_slug} exists={exists}\n"
                f"  url=https://polymarket.com/event/{event_slug}"
            )
            fd = _forecast_diag(city_name)
            print(
                f"  station={fd['station_code']} ({fd['station_name']}) source={fd['source_type']} "
                f"forecast=({fd['forecast_lat']:.4f},{fd['forecast_lon']:.4f}) mode={fd['city_mode']}"
            )
            print(f"  resolution_url={fd['resolution_url']}")
            if not exists:
                continue
            res = evaluate_signal_for_event(event_slug)
            if res.get("skip"):
                print(f"SKIP reason={res.get('reason')} st={res.get('station_code')}")
                continue
            print(
                f"ensemble_p={res['p_main']:.4f} ({res['p_main_members']}/51) "
                f"market_main={res['main']['yes']:.4f} gap_ana={res['gap']:+.4f}"
            )
            print(
                f"pricing: analytical={res.get('analytical_price')} entry_assumed={res.get('entry_price_assumed')} "
                f"bb/ba={res.get('best_bid_main')}/{res.get('best_ask_main')} spread={res.get('spread_main')} "
                f"vol_main={res.get('volume_main')}"
            )
            ga = res.get("gap_analytical")
            ge = res.get("gap_entry")
            if isinstance(ga, (int, float)) and isinstance(ge, (int, float)):
                print(f"gaps: gap_analytical={ga:+.4f} gap_entry={ge:+.4f}")
            else:
                print(f"gaps: gap_analytical={ga} gap_entry={ge}")
            print(
                f"rules: gap_thr={res['gap_threshold']} main_max={res['rules']['main_max']} "
                f"nb_max={res['rules']['neighbor_max']} stake=${res['stake']}"
            )
            nb_prices = []
            if res.get('low') is not None:
                nb_prices.append(res['low']['yes'])
            if res.get('high') is not None:
                nb_prices.append(res['high']['yes'])
            print(f"neighbors_max={max(nb_prices) if nb_prices else None} ok_prices={res['ok_prices']}")
            print(
                f"ok_gap={res['ok_gap']} ok_liquidity={res.get('ok_liquidity')} ok_confidence={res['ok_confidence']} "
                f"high_conf={res['high_confidence']}"
            )
            print(f"signal={res['signal']} structure_type={res.get('structure_type')} reasons={res.get('reasons')}")
    print("\n=== конец ===")


def cmd_backtest(days: int = 7) -> None:
    """
    --backtest: прогон последних N дней (приближенно).
    Интерпретация:
    - "в теории": сколько сигналов было бы по базовым условиям (цены+порог разрыва)
    - "бот отправил бы": сколько прошло бы дополнительно confidence-фильтр (ok_confidence)

    Важно: это НЕ исторические цены Polymarket во времени (Gamma не даёт их тут).
    Используются текущие снимки рынков на момент запуска.
    """
    theory = 0
    would_send = 0
    missed: dict = {}

    # Кэши, чтобы не долбить API одинаковыми запросами
    found_cache: dict[str, bool] = {}
    res_cache: dict[tuple[tuple[str, dt.date], str], dict | None] = {}

    # Чтобы не упереться в лимиты Open‑Meteo, по умолчанию делаем мягкий режим:
    # ограничим количество дней до 3, если не задано другое.
    # Open‑Meteo часто отвечает 429 при большом числе запросов; 7 дней × города × 3 глубины
    # даёт слишком много вызовов. По умолчанию ограничиваем окно (можно расширить позже с паузами).
    days_effective = min(days, 3)

    for back in range(1, days_effective + 1):
        print(f"backtest: day -{back}", flush=True)
        for city in _enabled_city_dicts():
            city_name = city["name"]
            # Якорь «сегодня» в календаре города (как в бою), от него D+1..3
            anchor = _local_today_for_city(city_name) - dt.timedelta(days=back)
            for depth in (1, 2, 3):
                d = anchor + dt.timedelta(days=depth)
                event_slug = build_event_slug(city_name, d)
                if event_slug not in found_cache:
                    found_cache[event_slug] = check_event_exists(event_slug)
                if not found_cache[event_slug]:
                    continue
                cache_key = ((city_name, anchor), event_slug)
                if cache_key in res_cache:
                    res = res_cache[cache_key]
                else:
                    try:
                        res = evaluate_signal_for_event(event_slug, run_date=anchor)
                    except AssertionError:
                        res = None
                    except RuntimeError:
                        # обычно это 429 rate limit
                        res = None
                    res_cache[cache_key] = res
                if res is None:
                    _inc(missed, "backtest_failed_or_rate_limited")
                    continue
                if res.get("skip"):
                    _inc(missed, f"skip_{res.get('reason')}")
                    continue
                base_ok = bool(res["ok_prices"] and res["ok_gap"])
                if base_ok:
                    theory += 1
                    if res["signal"]:
                        would_send += 1
                    else:
                        # почему "пропустили" при текущих правилах
                        for r in res.get("reasons", []):
                            if r != "ok":
                                _inc(missed, r)

    print(f"=== BACKTEST (запрошено дней: {days}, фактически: {days_effective}; см. лимит API) ===")
    print(f"Сигналов в теории (base ok): {theory}")
    print(f"Сигналов бот отправил бы (текущие правила): {would_send}")
    skipped = theory - would_send
    print(f"Пропущено сигналов: {skipped}")
    if missed:
        print("Причины пропуска/скипа:")
        for k in sorted(missed.keys()):
            print(f"- {k}: {missed[k]}")
    print("=== конец ===")

    # сохраним в stats.json для будущего ежедневного отчёта
    stats = load_stats()
    stats["last_backtest"] = {
        "at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "days_requested": days,
        "days_effective": days_effective,
        "theory": theory,
        "would_send": would_send,
        "missed_breakdown": missed,
    }
    save_stats(stats)


def _format_bucket_label(bucket: dict, unit: str) -> str:
    low = bucket.get("low")
    high = bucket.get("high")
    if low is None and high is not None:
        return f"{high}°{unit} or below"
    if high is None and low is not None:
        return f"{low}°{unit} or higher"
    if low is not None and high is not None and low == high:
        return f"{low}°{unit}"
    if low is not None and high is not None:
        return f"{low}-{high}°{unit}"
    return "bucket"


def _split_stake_70_15_15(total: int) -> tuple[int, int, int]:
    """
    Возвращает (low, main, high) суммы в $.
    Округляем до $1, затем подгоняем, чтобы сумма ровно совпала с total.
    """
    if total <= 0:
        return 0, 0, 0
    main = int(round(total * 0.70))
    low = int(round(total * 0.15))
    high = int(round(total * 0.15))
    s = low + main + high
    # Подгоняем остаток/перебор в main (самое логичное место)
    main += (total - s)
    if main < 0:
        main = 0
    # если после этого сумма всё ещё не совпала (крайние случаи), раскидаем по соседям
    s2 = low + main + high
    if s2 != total:
        delta = total - s2
        if delta > 0:
            high += delta
        else:
            # уменьшаем high/low по очереди
            for _ in range(-delta):
                if high > 0:
                    high -= 1
                elif low > 0:
                    low -= 1
                elif main > 0:
                    main -= 1
    return low, main, high


def _format_date_ru(date_iso: str) -> str:
    y, m, d = date_iso.split("-")
    return f"{int(d)} {MONTH_RU.get(int(m), m)}"


def _sell_target_multiplier(depth: int) -> float:
    return {1: 0.95, 2: 0.90, 3: 0.85}[depth]


def build_signal_message(res: dict) -> str:
    city_en = res["city"]
    city = CITY_RU.get(city_en, city_en)
    depth = int(res["depth"])
    date_iso = res["date"]
    date_human = _format_date_ru(date_iso)

    p = float(res["p_main"])
    mkt = float(res["main"]["yes"])
    gap = float(res["gap"])

    stake = int(res["stake"])
    low_amt, main_amt, high_amt = _split_stake_70_15_15(stake)

    unit = res["unit"]
    low_b = res["low"]
    main_b = res["main"]
    high_b = res["high"]
    assert low_b is not None and high_b is not None

    target = round(p * _sell_target_multiplier(depth), 2)

    event_slug = res["event_slug"]
    url = f"https://polymarket.com/event/{event_slug}"

    prefix = "🔥 СИГНАЛ"
    if res.get("high_confidence"):
        prefix = "🔥 ВЫСОКАЯ УВЕРЕННОСТЬ"
    lines = []
    lines.append(f"{prefix}: {city} | D+{depth} ({date_human})")
    if res.get("station_code"):
        lines.append(
            f"📍 {res['station_code']} {res.get('station_name', '')} · {res.get('source_type', '')} · "
            f"forecast {res.get('forecast_lat'):.4f},{res.get('forecast_lon'):.4f} · mode={res.get('city_mode', '')}"
        )
        lines.append(f"🔗 resolve: {res.get('resolution_url', '')}")
    lines.append("")
    mb = main_b
    mbb, mba = mb.get("best_bid"), mb.get("best_ask")
    mkt_line = f"📊 Ensemble: {p*100:.0f}% ({res['p_main_members']}/51) | Mid (analytical): {mkt:.2f}"
    if mbb is not None and mba is not None:
        mkt_line += f" | bid {mbb:.2f} / ask {mba:.2f}"
    mkt_line += f" | gap_ana: {gap*100:+.0f}%"
    lines.append(mkt_line)
    if res.get("entry_price_assumed") is not None:
        sp = res.get("spread_main")
        sp_txt = f"{sp:.3f}" if isinstance(sp, (int, float)) else "n/a"
        ge = res.get("gap_entry")
        ge_txt = f"{float(ge)*100:+.0f}%" if ge is not None else "n/a"
        lines.append(
            f"💵 Entry (assumed): {float(res['entry_price_assumed']):.2f} | spread {sp_txt} | gap_entry {ge_txt}"
        )
    lines.append("")
    lines.append("🎯 ПОКУПАЙ ЛЕСТНИЦУ:")
    lines.append(_format_bucket_telegram_line(_format_bucket_label(low_b, unit), low_b, low_amt))
    lines.append(_format_bucket_telegram_line(_format_bucket_label(main_b, unit), main_b, main_amt))
    lines.append(_format_bucket_telegram_line(_format_bucket_label(high_b, unit), high_b, high_amt))
    lines.append("")
    lines.append(f"💰 Цель продажи: {target:.2f}")
    lines.append("")
    lines.append(f"🔗 {url}")
    lines.append("")
    lines.append("---")
    lines.append("🌧 Дождь идёт, бабло течёт. Like A Boss.")
    return "\n".join(lines)


async def send_telegram_text(
    text: str,
    *,
    reply_to_message_id: int | None = None,
) -> int | None:
    """
    Отправка в TELEGRAM_CHAT_ID. Возвращает message_id отправленного сообщения.
    reply_to_message_id — опционально (для обычных сигналов не используется).
    """
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        raise SystemExit("Нужно задать TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID.")
    bot = Bot(token=token)
    kw: dict = {"chat_id": chat_id, "text": text}
    if reply_to_message_id is not None:
        kw["reply_to_message_id"] = int(reply_to_message_id)
    msg = await bot.send_message(**kw)
    return int(msg.message_id)


async def send_telegram_text_reply_fallback(text: str, reply_to_message_id: int | None) -> None:
    """
    Для paper-событий: сначала reply на корневой сигнал; при любой ошибке Telegram — обычное сообщение.
    """
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        raise SystemExit("Нужно задать TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID.")
    bot = Bot(token=token)
    if reply_to_message_id is not None:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_to_message_id=int(reply_to_message_id),
            )
            return
        except TelegramError as e:
            log.warning(
                "telegram reply_to=%s не принят (%s), отправляю без reply",
                reply_to_message_id,
                e,
            )
        except Exception as e:
            log.warning(
                "telegram reply_to=%s: неожиданная ошибка (%s), отправляю без reply",
                reply_to_message_id,
                e,
            )
    await bot.send_message(chat_id=chat_id, text=text)


def send_paper_telegram_safe(text: str, *, event_slug: str | None = None) -> None:
    """
    Уведомления paper: не падает и не роняет движок при отсутствии токена / сетевой ошибке.
    Если передан event_slug — по возможности reply на сохранённый корневой сигнал.
    """
    try:
        from paper_settings import paper_telegram_notifications_enabled, paper_trading_enabled
        from telegram_signal_linkage import get_signal_thread_root

        if not paper_trading_enabled() or not paper_telegram_notifications_enabled():
            return
        reply_to: int | None = None
        if event_slug:
            root = get_signal_thread_root(event_slug)
            env_chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
            if root and env_chat and str(root.get("chat_id")) == str(env_chat):
                reply_to = int(root["message_id"])
            elif root and env_chat:
                log.debug(
                    "telegram_signal_linkage: chat_id не совпал с TELEGRAM_CHAT_ID, reply отключён для %s",
                    event_slug,
                )
        if reply_to is not None:
            asyncio.run(send_telegram_text_reply_fallback(text, reply_to))
        else:
            asyncio.run(send_telegram_text(text))
    except SystemExit:
        log.warning("paper telegram: пропуск (нет TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
    except Exception as e:
        log.warning("paper telegram: %s", e)


def cmd_send_signals_once() -> None:
    """Один проход без дедупа (ручной запуск)."""
    sent = run_signals_round(respect_dedup=False)
    print(f"Отправлено сигналов в Telegram: {sent}")

def dump_polymarket_event_ladder(event_slug: str) -> None:
    """
    Печатает "лестницу" события: список рынков (диапазонов) и вероятность (цена YES).
    Это соответствует тому, что ты видишь на странице события: много строк типа
    '56°F or below', '57-58°F', ... каждая строка = отдельный рынок Yes/No.
    """
    event = _gamma_get_event_by_slug(event_slug)
    markets = event["markets"]

    rows: list[tuple[str, dict]] = []
    for m in markets:
        if not isinstance(m, dict):
            continue
        question = m.get("question") or m.get("title") or ""
        if not isinstance(question, str) or not question.strip():
            continue

        outcomes = _extract_outcomes_field(m)
        prices = _extract_outcome_prices_field(m)
        if len(outcomes) != 2 or len(prices) != 2:
            # Для "лестницы" ожидаем бинарные рынки Yes/No
            continue

        label = question
        rows.append((label, _yes_price_and_liquidity(m)))

    assert len(rows) > 0, "Ошибка: не удалось собрать лестницу (не нашли бинарные рынки Yes/No)."

    print("=== Polymarket EVENT ladder (mid / bid / ask / vol) ===")
    print(f"event_slug: {event_slug}")
    title = event.get("title") or event.get("name") or ""
    if isinstance(title, str) and title.strip():
        print(f"title: {title}")
    for label, liq in rows:
        bb, ba = liq.get("best_bid"), liq.get("best_ask")
        vol = liq.get("volume")
        extra = f"mid={liq['yes']:.4f} outcome={liq.get('outcome_yes', 0):.4f}"
        if bb is not None and ba is not None:
            extra += f" bid={bb:.4f} ask={ba:.4f}"
        if vol:
            extra += f" vol=${vol:,.0f}".replace(",", " ")
        print(f"- {label} | {extra}")
    print("=== конец ===")


def find_active_weather_events(city: str, limit_pages: int = 5, page_size: int = 100) -> list[dict]:
    """
    Ищет активные (active=true, closed=false) события про highest temperature для города.
    Возвращает список dict: {slug,title,endDate}.
    """
    city_lc = city.strip().lower()
    assert city_lc, "Ошибка: city пустой."

    hits: list[dict] = []
    offset = 0
    for _ in range(limit_pages):
        r = requests.get(
            f"{POLYMARKET_GAMMA_BASE}/events",
            params={
                "active": "true",
                "closed": "false",
                "limit": page_size,
                "offset": offset,
            },
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list) or len(data) == 0:
            break

        for ev in data:
            if not isinstance(ev, dict):
                continue
            title = ev.get("title") or ev.get("name") or ""
            slug = ev.get("slug") or ""
            if not isinstance(title, str) or not isinstance(slug, str):
                continue
            t = title.lower()
            if "highest temperature" in t and city_lc in t:
                hits.append(
                    {
                        "slug": slug,
                        "title": title,
                        "endDate": ev.get("endDate"),
                    }
                )

        offset += page_size

    return hits


def cmd_find_events() -> None:
    city = os.environ.get("POLYMARKET_CITY", "").strip()
    if not city:
        raise SystemExit(
            "Нужен город.\n"
            "Вариант 1: $env:POLYMARKET_CITY=\"London\"\n"
            "Вариант 2: python main.py --find-events London"
        )
    hits = find_active_weather_events(city)
    if not hits:
        print(f"Не нашёл активных событий highest temperature для города: {city}")
        return
    print("=== Active events found ===")
    for ev in hits[:30]:
        print(f"- {ev['slug']} | {ev['title']} | endDate={ev.get('endDate')}")
    print("=== конец ===")


def _try_find_any_weather_slug() -> str:
    """
    Пытаемся автоматически найти любой рынок про 'highest temperature' в активных.
    Если не нашли — просим пользователя задать POLYMARKET_TEST_SLUG.
    """
    markets = requests.get(
        f"{POLYMARKET_GAMMA_BASE}/markets",
        params={"active": "true", "limit": 2000},
        timeout=30,
    ).json()
    if not isinstance(markets, list):
        return ""
    for m in markets:
        if not isinstance(m, dict):
            continue
        slug = m.get("slug")
        question = (m.get("question") or m.get("title") or "")
        if isinstance(slug, str) and isinstance(question, str):
            q = question.lower()
            if "highest temperature" in q:
                return slug
    return ""


def test_polymarket() -> tuple[str, list[float]]:
    slug = os.environ.get("POLYMARKET_TEST_SLUG", "").strip()
    if not slug:
        slug = _try_find_any_weather_slug()
    if not slug:
        raise AssertionError(
            "Не удалось автоматически найти погодный рынок в Gamma API.\n"
            "Задай переменную окружения POLYMARKET_TEST_SLUG (slug рынка) и запусти selftest ещё раз."
        )

    data = _gamma_get_market_by_slug(slug)
    prices_float = _extract_outcome_prices_field(data)

    print(f"[OK] Polymarket: slug={slug} outcome_prices={prices_float[:10]}")
    return slug, prices_float


def dump_polymarket_buckets(slug: str) -> None:
    """
    Этап 3 (из ТЗ): подключение к Polymarket Gamma API и вывод бакетов.
    """
    data = _gamma_get_market_by_slug(slug)
    outcomes = _extract_outcomes_field(data)
    prices = _extract_outcome_prices_field(data)

    assert len(outcomes) == len(prices), (
        f"Ошибка: длина outcomes ({len(outcomes)}) не совпала с outcome prices ({len(prices)})."
    )

    print("=== Polymarket: бакеты и цены ===")
    print(f"slug: {slug}")
    q = data.get("question") or data.get("title") or ""
    if isinstance(q, str) and q.strip():
        print(f"question: {q}")
    for name, price in zip(outcomes, prices):
        print(f"- {name}: {price}")
    print("=== конец ===")


def test_matching() -> None:
    forecast = 20.5
    buckets = ["13°C or below", "14°C", "15°C", "16°C", "17°C", "18°C", "19°C", "20°C", "21°C", "22°C or higher"]
    prices = [0.01, 0.02, 0.03, 0.05, 0.08, 0.12, 0.18, 0.35, 0.14, 0.02]

    def extract_temp(bucket_name: str) -> int:
        # В наших погодных рынках температура всегда до символа "°"
        return int(bucket_name.split("°")[0])

    bucket_temps = [extract_temp(b) for b in buckets]
    closest_idx = min(range(len(bucket_temps)), key=lambda i: abs(bucket_temps[i] - forecast))

    print(
        f"[OK] Matching: прогноз {forecast}°C -> ближайший бакет {buckets[closest_idx]} "
        f"(темп {bucket_temps[closest_idx]}°C, цена {prices[closest_idx]})"
    )

    if closest_idx == 0 or closest_idx == len(buckets) - 1:
        raise AssertionError("Ошибка: ближайший бакет на краю списка — нет двух соседей для проверки.")

    print(f"   Нижний сосед: {buckets[closest_idx-1]} (цена {prices[closest_idx-1]})")
    print(f"   Верхний сосед: {buckets[closest_idx+1]} (цена {prices[closest_idx+1]})")


def run_selftests() -> None:
    print("=== SELFTEST: критические проверки парсинга ===")
    test_openmeteo()
    test_polymarket()
    test_matching()
    print("[OK] SELFTEST: все проверки пройдены")


def main() -> None:
    setup_logging()
    if "--debug" in sys.argv:
        cmd_debug()
        return

    if "--backtest" in sys.argv:
        cmd_backtest(7)
        return

    if "--send-signals-once" in sys.argv:
        cmd_send_signals_once()
        return

    if "--signal-check" in sys.argv:
        cmd_signal_check()
        return

    if "--dump-openmeteo" in sys.argv:
        # Использование:
        # python main.py --dump-openmeteo
        # python main.py --dump-openmeteo Paris
        idx = sys.argv.index("--dump-openmeteo")
        city = sys.argv[idx + 1].strip() if idx + 1 < len(sys.argv) else "Paris"
        dump_openmeteo_example(city_name=city)
        return

    if "--analyze-event" in sys.argv:
        idx = sys.argv.index("--analyze-event")
        slug = sys.argv[idx + 1].strip() if idx + 1 < len(sys.argv) else ""
        if not slug:
            raise SystemExit("Использование: python main.py --analyze-event <event_slug>")
        analyze_event_vs_ecmwf(slug)
        return

    if "--check-slugs" in sys.argv:
        cmd_check_slugs()
        return

    if "--find-events" in sys.argv:
        if "POLYMARKET_CITY" not in os.environ:
            idx = sys.argv.index("--find-events")
            if idx + 1 < len(sys.argv):
                os.environ["POLYMARKET_CITY"] = sys.argv[idx + 1]
        cmd_find_events()
        return

    if "--dump-event" in sys.argv:
        idx = sys.argv.index("--dump-event")
        slug = sys.argv[idx + 1].strip() if idx + 1 < len(sys.argv) else ""
        if not slug:
            slug = os.environ.get("POLYMARKET_TEST_SLUG", "").strip()
        if not slug:
            raise SystemExit(
                "Нужен event slug.\n"
                "Вариант 1: $env:POLYMARKET_TEST_SLUG=\"<event-slug>\"\n"
                "Вариант 2: python main.py --dump-event <event-slug>"
            )
        dump_polymarket_event_ladder(slug)
        return

    if "--dump-polymarket" in sys.argv:
        idx = sys.argv.index("--dump-polymarket")
        slug = sys.argv[idx + 1].strip() if idx + 1 < len(sys.argv) else ""
        if not slug:
            slug = os.environ.get("POLYMARKET_TEST_SLUG", "").strip()
        if not slug:
            raise SystemExit(
                "Нужен slug.\n"
                "Вариант 1: $env:POLYMARKET_TEST_SLUG=\"<slug>\"\n"
                "Вариант 2: python main.py --dump-polymarket <slug>"
            )
        dump_polymarket_buckets(slug)
        return

    if "--selftest" in sys.argv:
        run_selftests()
        return

    # 1) Пинг в Telegram при запуске (как в ТЗ этап 1)
    asyncio.run(send_startup_message())
    # 2) Погода в консоль (как в ТЗ этап 2)
    log_weather_once()

    # 3) Telegram: /status, /restart
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    app: Application = ApplicationBuilder().token(token).build()
    app.add_handler(CommandHandler("status", on_status))
    app.add_handler(CommandHandler("paper", on_paper))
    app.add_handler(CommandHandler("restart", on_restart))

    # 4) Фоновый опрос сигналов (реальное время)
    _start_periodic_signals()

    _record_last_round_finished()

    app.run_polling(close_loop=False)


if __name__ == "__main__":
    setup_logging()
    _install_exception_hooks()
    main()
