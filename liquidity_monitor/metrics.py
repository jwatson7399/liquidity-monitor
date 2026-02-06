"""Net liquidity and derived metrics."""

from __future__ import annotations

from datetime import datetime, timedelta

from . import storage


def compute_net_liquidity(fed_assets: float, tga: float, reverse_repos: float) -> float:
    """Net Liquidity = Fed Balance Sheet - TGA - Reverse Repos.

    All values are in millions (FRED's native unit for these series).
    """
    return fed_assets - tga - reverse_repos


def get_net_liquidity_history(conn, days: int = 90) -> list[dict]:
    """Build a net liquidity time series from stored data.

    Aligns on dates where all three components have values.
    """
    walcl = {r["date"]: r["value"] for r in storage.get_series_history(conn, "WALCL", days)}
    tga = {r["date"]: r["value"] for r in storage.get_series_history(conn, "WTREGEN", days)}
    rrp = {r["date"]: r["value"] for r in storage.get_series_history(conn, "RRPONTSYD", days)}

    # RRPONTSYD is in billions, others in millions — normalize to millions
    common_dates = sorted(set(walcl) & set(tga) & set(rrp))
    history = []
    for d in common_dates:
        nl = compute_net_liquidity(walcl[d], tga[d], rrp[d] * 1000)
        history.append({"date": d, "value": nl})
    return history


def get_current_snapshot(conn) -> dict:
    """Get the latest value and changes for each series plus net liquidity."""
    today = datetime.now()
    week_ago = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    month_ago = (today - timedelta(days=30)).strftime("%Y-%m-%d")

    from .fred_client import SERIES

    snapshot = {}
    for series_id, label in SERIES.items():
        latest_rows = storage.get_latest(conn, series_id, limit=1)
        if not latest_rows:
            snapshot[series_id] = {"label": label, "current": None}
            continue

        current = latest_rows[0]
        week_val = storage.get_value_at_offset(conn, series_id, week_ago)
        month_val = storage.get_value_at_offset(conn, series_id, month_ago)

        entry = {
            "label": label,
            "current_date": current["date"],
            "current": current["value"],
            "week_ago": week_val["value"] if week_val else None,
            "month_ago": month_val["value"] if month_val else None,
        }

        if entry["week_ago"] is not None:
            entry["week_change"] = entry["current"] - entry["week_ago"]
        if entry["month_ago"] is not None:
            entry["month_change"] = entry["current"] - entry["month_ago"]

        snapshot[series_id] = entry

    # Net liquidity
    w = snapshot.get("WALCL", {})
    t = snapshot.get("WTREGEN", {})
    r = snapshot.get("RRPONTSYD", {})

    if w.get("current") is not None and t.get("current") is not None and r.get("current") is not None:
        nl_current = compute_net_liquidity(w["current"], t["current"], r["current"] * 1000)
        nl_entry = {"label": "Net Liquidity", "current": nl_current, "current_date": w["current_date"]}

        if w.get("week_ago") and t.get("week_ago") and r.get("week_ago"):
            nl_week = compute_net_liquidity(w["week_ago"], t["week_ago"], r["week_ago"] * 1000)
            nl_entry["week_ago"] = nl_week
            nl_entry["week_change"] = nl_current - nl_week

        if w.get("month_ago") and t.get("month_ago") and r.get("month_ago"):
            nl_month = compute_net_liquidity(w["month_ago"], t["month_ago"], r["month_ago"] * 1000)
            nl_entry["month_ago"] = nl_month
            nl_entry["month_change"] = nl_current - nl_month

        snapshot["NET_LIQUIDITY"] = nl_entry

    return snapshot
