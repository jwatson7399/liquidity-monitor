#!/usr/bin/env python3
"""Generate a static HTML dashboard and write it to docs/index.html.

Used by GitHub Actions to publish to GitHub Pages.
Fetches fresh data from FRED + CoinGecko, computes metrics, and bakes
everything into a single self-contained HTML file.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from liquidity_monitor.fred_client import fetch_all, get_api_key, SERIES, ALL_FRED_SERIES
from liquidity_monitor.crypto_client import fetch_all_crypto
from liquidity_monitor.storage import get_connection, init_db, upsert_observations
from liquidity_monitor.metrics import (
    get_current_snapshot,
    get_net_liquidity_history,
    get_global_liquidity_history,
    get_stablecoin_history,
    get_btc_history,
    get_eth_history,
    get_altcoin_history,
    get_liquidity_impulse,
    get_regime,
)
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

    # Fetch FRED data
    print("Fetching from FRED API...")
    fred_data = fetch_all(api_key)
    for series_id, observations in fred_data.items():
        n = upsert_observations(conn, series_id, observations)
        label = ALL_FRED_SERIES.get(series_id, series_id)
        print(f"  {label}: {len(observations)} obs, {n} upserted")

    # Fetch crypto data
    print("Fetching from CoinGecko...")
    crypto_data = fetch_all_crypto()
    for series_id, observations in crypto_data.items():
        n = upsert_observations(conn, series_id, observations)
        label = series_id
        print(f"  {label}: {len(observations)} obs, {n} upserted")

    # Build all metrics
    snapshot = get_current_snapshot(conn)
    net_liq_history = get_net_liquidity_history(conn)
    global_liq_history = get_global_liquidity_history(conn)
    stablecoin_history = get_stablecoin_history(conn)
    btc_history = get_btc_history(conn)
    eth_history = get_eth_history(conn)
    altcoin_history = get_altcoin_history(conn)
    impulse = get_liquidity_impulse(net_liq_history)
    regime = get_regime(impulse)

    conn.close()

    # US metrics table
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

    # Net liquidity chart data (in trillions)
    chart_net_liq = [
        {"date": p["date"], "value": round(p["value"] / 1e6, 4)}
        for p in net_liq_history
    ]

    # Summary cards
    summary = {}

    # Net liquidity summary
    nl = snapshot.get("NET_LIQUIDITY", {})
    if nl.get("current") is not None:
        summary["net_liquidity"] = round(nl["current"] / 1e6, 3)

    # BTC summary
    if btc_history:
        btc_latest = btc_history[-1]["value"]
        summary["btc_price"] = round(btc_latest, 0)
        # 30d pct change
        if len(btc_history) > 30:
            btc_30d = btc_history[-31]["value"]
            summary["btc_30d_pct"] = round((btc_latest - btc_30d) / btc_30d * 100, 1)

    # Stablecoin summary
    if stablecoin_history:
        stable_latest = stablecoin_history[-1]["value"]
        summary["stablecoin_total"] = round(stable_latest, 1)
        if len(stablecoin_history) > 30:
            stable_30d = stablecoin_history[-31]["value"]
            summary["stablecoin_30d_change"] = round(stable_latest - stable_30d, 1)

    # Global liquidity summary
    if global_liq_history:
        summary["global_liquidity"] = global_liq_history[-1]["value"]
        if len(global_liq_history) > 4:
            gl_prev = global_liq_history[-5]["value"]
            gl_curr = global_liq_history[-1]["value"]
            summary["global_30d_pct"] = round((gl_curr - gl_prev) / gl_prev * 100, 1)

    # ETH summary
    if eth_history:
        eth_latest = eth_history[-1]["value"]
        summary["eth_price"] = round(eth_latest, 0)
        if len(eth_history) > 30:
            eth_30d = eth_history[-31]["value"]
            summary["eth_30d_pct"] = round((eth_latest - eth_30d) / eth_30d * 100, 1)

    # Altcoin (ETH mcap) summary
    if altcoin_history:
        alt_latest = altcoin_history[-1]["value"]
        summary["altcoin_mcap"] = round(alt_latest, 1)
        if len(altcoin_history) > 30:
            alt_30d = altcoin_history[-31]["value"]
            summary["altcoin_30d_pct"] = round((alt_latest - alt_30d) / alt_30d * 100, 1)

    return {
        "table": table_rows,
        "chart_net_liq": chart_net_liq,
        "chart_btc": btc_history,
        "chart_eth": eth_history,
        "chart_altcoins": altcoin_history,
        "chart_stablecoin": stablecoin_history,
        "chart_global_liq": global_liq_history,
        "regime": regime,
        "impulse": impulse,
        "summary": summary,
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
