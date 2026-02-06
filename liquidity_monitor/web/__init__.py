"""Web dashboard for the Liquidity Monitor."""

from __future__ import annotations

from flask import Flask, jsonify, render_template

from ..fred_client import fetch_all, get_api_key, SERIES, ALL_FRED_SERIES
from ..crypto_client import fetch_all_crypto
from ..storage import get_connection, init_db, upsert_observations
from ..metrics import (
    get_current_snapshot,
    get_net_liquidity_history,
    get_global_liquidity_history,
    get_stablecoin_history,
    get_ism_history,
    get_btc_history,
    get_eth_history,
    get_altcoin_history,
    get_liquidity_impulse,
    get_regime,
)
from ..report import DISPLAY_ORDER, UNITS


def _build_dashboard_data() -> dict:
    """Gather all data needed for the dashboard."""
    conn = get_connection()
    init_db(conn)

    snapshot = get_current_snapshot(conn)
    net_liq_history = get_net_liquidity_history(conn)
    global_liq_history = get_global_liquidity_history(conn)
    stablecoin_history = get_stablecoin_history(conn)
    ism_history = get_ism_history(conn)
    btc_history = get_btc_history(conn)
    eth_history = get_eth_history(conn)
    altcoin_history = get_altcoin_history(conn)
    impulse = get_liquidity_impulse(net_liq_history)
    regime = get_regime(impulse)

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
            "week_change": _fmt_billions(entry.get("week_change"), divisor),
            "month_change": _fmt_billions(entry.get("month_change"), divisor),
        })

    chart_net_liq = [
        {"date": p["date"], "value": round(p["value"] / 1e6, 4)}
        for p in net_liq_history
    ]

    # Summary
    summary = {}
    nl = snapshot.get("NET_LIQUIDITY", {})
    if nl.get("current") is not None:
        summary["net_liquidity"] = round(nl["current"] / 1e6, 3)
    if btc_history:
        summary["btc_price"] = round(btc_history[-1]["value"], 0)
        if len(btc_history) > 30:
            btc_30d = btc_history[-31]["value"]
            summary["btc_30d_pct"] = round((btc_history[-1]["value"] - btc_30d) / btc_30d * 100, 1)
    if stablecoin_history:
        summary["stablecoin_total"] = round(stablecoin_history[-1]["value"], 1)
        if len(stablecoin_history) > 30:
            summary["stablecoin_30d_change"] = round(stablecoin_history[-1]["value"] - stablecoin_history[-31]["value"], 1)
    if global_liq_history:
        summary["global_liquidity"] = global_liq_history[-1]["value"]
        if len(global_liq_history) > 4:
            gl_prev = global_liq_history[-5]["value"]
            summary["global_30d_pct"] = round((global_liq_history[-1]["value"] - gl_prev) / gl_prev * 100, 1)
    if eth_history:
        summary["eth_price"] = round(eth_history[-1]["value"], 0)
        if len(eth_history) > 30:
            eth_30d = eth_history[-31]["value"]
            summary["eth_30d_pct"] = round((eth_history[-1]["value"] - eth_30d) / eth_30d * 100, 1)
    if altcoin_history:
        summary["altcoin_mcap"] = round(altcoin_history[-1]["value"], 1)
        if len(altcoin_history) > 30:
            alt_30d = altcoin_history[-31]["value"]
            summary["altcoin_30d_pct"] = round((altcoin_history[-1]["value"] - alt_30d) / alt_30d * 100, 1)
    if ism_history:
        summary["ism"] = ism_history[-1]["value"]
        if len(ism_history) >= 2:
            summary["ism_prev"] = ism_history[-2]["value"]
            summary["ism_change"] = round(ism_history[-1]["value"] - ism_history[-2]["value"], 2)

    return {
        "table": table_rows,
        "chart_net_liq": chart_net_liq,
        "chart_btc": btc_history,
        "chart_eth": eth_history,
        "chart_altcoins": altcoin_history,
        "chart_ism": ism_history,
        "chart_stablecoin": stablecoin_history,
        "chart_global_liq": global_liq_history,
        "regime": regime,
        "impulse": impulse,
        "summary": summary,
    }


def _fmt_billions(value, divisor):
    if value is None:
        return None
    return round(value / (divisor / 1000), 1)


def create_app() -> Flask:
    app = Flask(__name__)

    @app.route("/")
    def dashboard():
        data = _build_dashboard_data()
        return render_template("dashboard.html", data=data)

    @app.route("/api/data")
    def api_data():
        return jsonify(_build_dashboard_data())

    @app.route("/api/refresh", methods=["POST"])
    def api_refresh():
        try:
            api_key = get_api_key()
        except SystemExit as e:
            return jsonify({"error": str(e)}), 400

        conn = get_connection()
        init_db(conn)

        fred_data = fetch_all(api_key)
        total = 0
        for series_id, observations in fred_data.items():
            total += upsert_observations(conn, series_id, observations)

        crypto_data = fetch_all_crypto()
        for series_id, observations in crypto_data.items():
            total += upsert_observations(conn, series_id, observations)

        conn.close()

        data = _build_dashboard_data()
        data["refreshed"] = total
        return jsonify(data)

    return app
