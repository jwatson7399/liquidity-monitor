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
    ("bitcoin", [("BTC_USD", "prices")]),
    ("ethereum", [("ETH_USD", "prices"), ("ETH_MCAP", "market_caps")]),
    ("tether", [("USDT_MCAP", "market_caps")]),
    ("usd-coin", [("USDC_MCAP", "market_caps")]),
]

# Free tier: 365 days max. With demo key: unlimited.
FREE_MAX_DAYS = 365
FULL_DAYS = 1825


def _get_max_days() -> int:
    return FULL_DAYS if os.environ.get("COINGECKO_API_KEY") else FREE_MAX_DAYS


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


def fetch_all_crypto() -> dict:
    """Fetch BTC/ETH prices, ETH mcap (altcoin proxy), and stablecoin mcaps.

    Returns {series_id: [observations]}.
    """
    days = _get_max_days()
    has_key = bool(os.environ.get("COINGECKO_API_KEY"))
    if not has_key:
        print(f"  (No COINGECKO_API_KEY — fetching {days}d. Set one for 5yr history)")

    results = {}
    for coin_id, extractions in CRYPTO_FETCHES:
        try:
            raw = _fetch_coin(coin_id, days)
            for series_id, field in extractions:
                results[series_id] = _extract_series(raw, field)
            time.sleep(6)  # respect free-tier rate limits
        except Exception as e:
            print(f"  Warning: failed to fetch {coin_id}: {e}")
            for series_id, _ in extractions:
                results[series_id] = []
    return results
