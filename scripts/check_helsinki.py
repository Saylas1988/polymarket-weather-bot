"""Одноразовая проверка: Open-Meteo + slug + Gamma для Helsinki."""
import datetime as dt
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import main as m


def main() -> None:
    city = "Helsinki"
    lat, lon = m._get_city_coords(city)
    local_today = m._local_today_for_city(city)
    lines = []
    lines.append("=== Helsinki: координаты станции (EFHK), Ensemble, Polymarket slug ===\n")
    lines.append(f"Координаты прогноза (resolving station): {lat}, {lon}")
    lines.append(f"Локальный сегодня (Europe/Helsinki): {local_today.isoformat()}\n")

    for depth in (1, 2, 3):
        d = local_today + dt.timedelta(days=depth)
        slug = m.build_event_slug(city, d)
        url = f"https://polymarket.com/event/{slug}"
        gamma = f"https://gamma-api.polymarket.com/events/slug/{slug}"
        lines.append(f"--- D+{depth} (дата события {d.isoformat()}) ---")
        lines.append(f"slug: {slug}")
        lines.append(f"Polymarket: {url}")
        lines.append(f"Gamma JSON: {gamma}")

        exists = m.check_event_exists(slug)
        lines.append(f"Событие в Gamma (200): {'да' if exists else 'НЕТ (404)'}")
        if not exists:
            lines.append("")
            continue
        try:
            unit, members = m.fetch_ensemble_tmax_members_for_date(
                city, d, forecast_days=7, past_days=0
            )
            lines.append(
                f"Ensemble: unit={unit!r}, control tmax={members[0]:.2f}, членов={len(members)}"
            )
            ev = m._gamma_get_event_by_slug(slug)
            title = ev.get("title") or ev.get("name") or ""
            mkts = ev.get("markets") or []
            lines.append(f"Gamma title: {title}")
            lines.append(f"Число рынков (бакетов): {len(mkts)}")
            if mkts and isinstance(mkts[0], dict):
                q = (mkts[0].get("question") or mkts[0].get("title") or "")[:120]
                lines.append(f"Пример вопроса [0]: {q}")
        except Exception as e:
            lines.append(f"ОШИБКА: {type(e).__name__}: {e}")
        lines.append("")

    out = "\n".join(lines)
    print(out)
    p = os.path.join(os.path.dirname(__file__), "helsinki_check_result.txt")
    with open(p, "w", encoding="utf-8") as f:
        f.write(out)
    print(f"\n(копия сохранена в {p})")


if __name__ == "__main__":
    main()
