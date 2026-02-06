"""CoinGecko client for BTC/ETH prices, altcoin mcap, and stablecoin supply."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

import requests

COINGECKO_BASE = "https://api.coingecko.com/api/v3"

# Each fetch: (coin_id, [(series_id, field), ...])
# Multiple fields extracted from a single API call to minimize requests.
CRYPTO_FETCHES = [
    ("bitcoin", [("BTC_USD", "prices"), ("BTC_MCAP", "market_caps")]),
    ("ethereum", [("ETH_USD", "prices"), ("ETH_MCAP", "market_caps")]),
    ("binancecoin", [("BNB_MCAP", "market_caps")]),
    ("solana", [("SOL_MCAP", "market_caps")]),
    ("ripple", [("XRP_MCAP", "market_caps")]),
    ("tether", [("USDT_MCAP", "market_caps")]),
    ("usd-coin", [("USDC_MCAP", "market_caps")]),
]

# CoinGecko demo plan: 365 days max. Demo key helps with rate limits.
MAX_DAYS = 365

# Altcoin mcap series that get summed as "tracked alts"
TRACKED_ALT_MCAPS = ["ETH_MCAP", "BNB_MCAP", "SOL_MCAP", "XRP_MCAP"]


def _get_headers() -> dict:
    headers = {"Accept": "application/json"}
    key = os.environ.get("COINGECKO_API_KEY")
    if key:
        headers["x-cg-demo-api-key"] = key
    return headers


def _extract_series(raw_data: dict, field: str) -> list[dict]:
    """Extract a date-value series from CoinGecko response."""
    observations = []
    seen_dates = set()
    for ts_ms, value in raw_data.get(field, []):
        date = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        if date in seen_dates:
            continue
        seen_dates.add(date)
        observations.append({"date": date, "value": value})
    return observations


def _fetch_coin(coin_id: str, days: int) -> dict:
    """Fetch raw market_chart data for a coin."""
    resp = requests.get(
        f"{COINGECKO_BASE}/coins/{coin_id}/market_chart",
        params={"vs_currency": "usd", "days": days},
        headers=_get_headers(),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_global_stats() -> dict | None:
    """Fetch current global crypto market stats from /global.

    Returns {"total_mcap": float, "btc_mcap_pct": float} or None.
    """
    try:
        resp = requests.get(
            f"{COINGECKO_BASE}/global",
            headers=_get_headers(),
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json().get("data", {})
        total = data.get("total_market_cap", {}).get("usd")
        btc_pct = data.get("market_cap_percentage", {}).get("btc")
        if total and btc_pct:
            return {"total_mcap": total, "btc_mcap_pct": btc_pct}
    except Exception as e:
        print(f"  Warning: failed to fetch /global: {e}")
    return None


def fetch_all_crypto() -> dict:
    """Fetch BTC/ETH prices, top altcoin mcaps, and stablecoin mcaps.

    Returns {series_id: [observations]}.
    """
    results = {}
    for coin_id, extractions in CRYPTO_FETCHES:
        try:
            raw = _fetch_coin(coin_id, MAX_DAYS)
            for series_id, field in extractions:
                results[series_id] = _extract_series(raw, field)
            time.sleep(6)  # respect free-tier rate limits
        except Exception as e:
            print(f"  Warning: failed to fetch {coin_id}: {e}")
            for series_id, _ in extractions:
                results[series_id] = []
    return results
