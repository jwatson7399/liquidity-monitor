"""Web dashboard for the Liquidity Monitor."""

from __future__ import annotations

from flask import Flask, jsonify, render_template

from ..fred_client import fetch_all, get_api_key, SERIES
from ..storage import get_connection, init_db, upsert_observations
from ..metrics import get_current_snapshot, get_net_liquidity_history
from ..report import DISPLAY_ORDER, UNITS


def _build_dashboard_data() -> dict:
    """Gather all data needed for the dashboard."""
    conn = get_connection()
    init_db(conn)
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
            "week_change": _fmt_billions(entry.get("week_change"), divisor),
            "month_change": _fmt_billions(entry.get("month_change"), divisor),
        })

    chart_data = [
        {"date": p["date"], "value": round(p["value"] / 1e6, 4)}
        for p in history
    ]

    return {"table": table_rows, "chart": chart_data}


def _fmt_billions(value, divisor):
    """Return change value in billions (number), or None."""
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
        all_data = fetch_all(api_key)
        total = 0
        for series_id, observations in all_data.items():
            total += upsert_observations(conn, series_id, observations)
        conn.close()

        data = _build_dashboard_data()
        data["refreshed"] = total
        return jsonify(data)

    return app
