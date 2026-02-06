#!/usr/bin/env python3
"""Generate a static HTML dashboard and write it to docs/index.html.

Used by GitHub Actions to publish to GitHub Pages.
Fetches fresh data from FRED, computes metrics, and bakes everything
into a single self-contained HTML file.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from liquidity_monitor.fred_client import fetch_all, get_api_key, SERIES
from liquidity_monitor.storage import get_connection, init_db, upsert_observations
from liquidity_monitor.metrics import get_current_snapshot, get_net_liquidity_history
from liquidity_monitor.report import DISPLAY_ORDER, UNITS

OUT_DIR = Path(__file__).parent / "docs"
TEMPLATE = Path(__file__).parent / "liquidity_monitor" / "web" / "templates" / "static_dashboard.html"


def fmt_billions(value, divisor):
    if value is None:
        return None
    return round(value / (divisor / 1000), 1)


def build_data() -> dict:
    api_key = get_api_key()
    conn = get_connection()
    init_db(conn)

    print("Fetching from FRED API...")
    all_data = fetch_all(api_key)
    for series_id, observations in all_data.items():
        n = upsert_observations(conn, series_id, observations)
        label = SERIES.get(series_id, series_id)
        print(f"  {label}: {len(observations)} obs, {n} upserted")

    snapshot = get_current_snapshot(conn)
    history = get_net_liquidity_history(conn, days=90)
    conn.close()

    table_rows = []
    for sid in DISPLAY_ORDER:
        entry = snapshot.get(sid)
        if not entry or entry.get("current") is None:
            continue
        _, divisor = UNITS.get(sid, ("M", 1e6))
        table_rows.append({
            "series_id": sid,
            "label": entry["label"],
            "current": round(entry["current"] / divisor, 4),
            "current_date": entry.get("current_date", ""),
            "week_change": fmt_billions(entry.get("week_change"), divisor),
            "month_change": fmt_billions(entry.get("month_change"), divisor),
        })

    chart_data = [
        {"date": p["date"], "value": round(p["value"] / 1e6, 4)}
        for p in history
    ]

    return {
        "table": table_rows,
        "chart": chart_data,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }


def render_html(data: dict) -> str:
    template = TEMPLATE.read_text()
    return template.replace("__DATA_PLACEHOLDER__", json.dumps(data))


def main():
    data = build_data()
    html = render_html(data)
    OUT_DIR.mkdir(exist_ok=True)
    out_path = OUT_DIR / "index.html"
    out_path.write_text(html)
    print(f"\nWrote {out_path} ({len(html):,} bytes)")


if __name__ == "__main__":
    main()
