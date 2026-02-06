"""Net liquidity and derived metrics."""

from __future__ import annotations

from datetime import datetime, timedelta

from . import storage


def compute_net_liquidity(fed_assets: float, tga: float, reverse_repos: float) -> float:
    """Net Liquidity = Fed Balance Sheet - TGA - Reverse Repos.

    All values are in millions (FRED's native unit for these series).
    """
    return fed_assets - tga - reverse_repos


def get_net_liquidity_history(conn, days: int = 2000) -> list[dict]:
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


def get_global_liquidity_history(conn, days: int = 2000) -> list[dict]:
    """Build global liquidity time series: Fed + ECB (converted to USD).

    Returns values in trillions of USD.
    """
    walcl = {r["date"]: r["value"] for r in storage.get_series_history(conn, "WALCL", days)}
    ecb = {r["date"]: r["value"] for r in storage.get_series_history(conn, "ECBASSETSW", days)}
    fx = {r["date"]: r["value"] for r in storage.get_series_history(conn, "DEXUSEU", days)}

    if not ecb or not fx:
        return []

    # Build a lookup for nearest FX rate (forward-fill)
    fx_dates = sorted(fx.keys())
    ecb_dates = sorted(ecb.keys())

    history = []
    fx_idx = 0
    for d in sorted(set(walcl) & set(ecb)):
        # Find closest FX rate on or before this date
        while fx_idx < len(fx_dates) - 1 and fx_dates[fx_idx + 1] <= d:
            fx_idx += 1
        if fx_idx >= len(fx_dates) or fx_dates[fx_idx] > d:
            continue
        rate = fx[fx_dates[fx_idx]]

        # Fed: millions USD -> trillions
        fed_t = walcl[d] / 1e6
        # ECB: millions EUR * USD/EUR rate -> millions USD -> trillions
        ecb_t = (ecb[d] * rate) / 1e6

        history.append({"date": d, "value": round(fed_t + ecb_t, 4)})

    return history


def get_stablecoin_history(conn, days: int = 2000) -> list[dict]:
    """Build total stablecoin supply time series (USDT + USDC).

    Returns values in billions of USD.
    """
    usdt = {r["date"]: r["value"] for r in storage.get_series_history(conn, "USDT_MCAP", days)}
    usdc = {r["date"]: r["value"] for r in storage.get_series_history(conn, "USDC_MCAP", days)}

    if not usdt:
        return []

    # Use USDT dates as base, add USDC where available
    all_dates = sorted(set(usdt) | set(usdc))
    history = []
    for d in all_dates:
        total = usdt.get(d, 0) + usdc.get(d, 0)
        if total > 0:
            history.append({"date": d, "value": round(total / 1e9, 2)})
    return history


def get_ism_history(conn, days: int = 2000) -> list[dict]:
    """Get NFCI (Financial Conditions) history.

    Kept as 'ism' naming for compatibility with the dashboard.
    NFCI: negative = loose conditions, positive = tight conditions.
    """
    rows = storage.get_series_history(conn, "NFCI", days)
    return [{"date": r["date"], "value": round(r["value"], 2)} for r in rows]


def get_btc_history(conn, days: int = 2000) -> list[dict]:
    """Get BTC price history."""
    rows = storage.get_series_history(conn, "BTC_USD", days)
    return [{"date": r["date"], "value": round(r["value"], 2)} for r in rows]


def get_eth_history(conn, days: int = 2000) -> list[dict]:
    """Get ETH price history."""
    rows = storage.get_series_history(conn, "ETH_USD", days)
    return [{"date": r["date"], "value": round(r["value"], 2)} for r in rows]


def get_altcoin_history(conn, global_stats=None, days: int = 2000) -> list[dict]:
    """Get total altcoin market cap history (ex-BTC), in billions of USD.

    Sums tracked alt mcaps (ETH, BNB, SOL, XRP), then scales up using
    a factor derived from CoinGecko /global stats to estimate the full
    altcoin market including the long tail of smaller coins.
    """
    from .crypto_client import TRACKED_ALT_MCAPS

    # Load all tracked alt mcap series
    alt_series = {}
    for sid in TRACKED_ALT_MCAPS:
        alt_series[sid] = {
            r["date"]: r["value"]
            for r in storage.get_series_history(conn, sid, days)
        }

    # BTC mcap for computing altcoin = total - btc
    btc_mcap = {
        r["date"]: r["value"]
        for r in storage.get_series_history(conn, "BTC_MCAP", days)
    }

    # Find dates where at least ETH_MCAP exists
    all_dates = sorted(alt_series.get("ETH_MCAP", {}).keys())
    if not all_dates:
        return []

    # Compute scaling factor: true_total_alts / tracked_alts_sum
    # Uses current /global data to estimate how much the long tail adds.
    scale = 1.0
    if global_stats and btc_mcap:
        total_mcap = global_stats["total_mcap"]
        btc_pct = global_stats["btc_mcap_pct"]
        true_alts = total_mcap * (1.0 - btc_pct / 100.0)

        # Sum tracked alts for latest date to compute ratio
        latest = all_dates[-1]
        tracked_sum = sum(
            s.get(latest, 0) for s in alt_series.values()
        )
        if tracked_sum > 0:
            scale = true_alts / tracked_sum

    history = []
    for d in all_dates:
        tracked_sum = sum(s.get(d, 0) for s in alt_series.values())
        if tracked_sum > 0:
            estimated_total = tracked_sum * scale
            history.append({"date": d, "value": round(estimated_total / 1e9, 2)})

    return history


def get_liquidity_impulse(net_liq_history: list[dict]) -> dict | None:
    """Compute 30-day liquidity impulse from net liquidity history.

    Returns {change_billions, change_pct} or None.
    """
    if len(net_liq_history) < 2:
        return None

    current = net_liq_history[-1]
    current_date = datetime.strptime(current["date"], "%Y-%m-%d")
    target = current_date - timedelta(days=30)

    # Find closest observation to 30 days ago
    closest = None
    for point in net_liq_history:
        d = datetime.strptime(point["date"], "%Y-%m-%d")
        if d <= target:
            closest = point
        else:
            break

    if closest is None:
        return None

    change = current["value"] - closest["value"]
    pct = (change / abs(closest["value"])) * 100 if closest["value"] != 0 else 0

    return {
        "change_billions": round(change / 1000, 1),  # millions -> billions
        "change_pct": round(pct, 2),
    }


def get_regime(impulse: dict | None) -> str:
    """Determine liquidity regime based on 30-day impulse.

    Returns 'expanding', 'contracting', or 'neutral'.
    """
    if impulse is None:
        return "neutral"
    # Threshold: +/- $20B over 30 days
    if impulse["change_billions"] > 20:
        return "expanding"
    elif impulse["change_billions"] < -20:
        return "contracting"
    return "neutral"


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
