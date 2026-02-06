"""FRED API client for fetching liquidity-related series."""

import os
from datetime import datetime, timedelta
from typing import Optional

import requests

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

SERIES = {
    "WALCL": "Fed Balance Sheet",
    "WRESBAL": "Bank Reserves",
    "RRPONTSYD": "Reverse Repos",
    "M2SL": "M2 Money Supply",
    "WTREGEN": "Treasury General Account",
}


def get_api_key() -> str:
    key = os.environ.get("FRED_API_KEY")
    if not key:
        raise SystemExit(
            "FRED_API_KEY not set. Get a free key at "
            "https://fred.stlouisfed.org/docs/api/api_key.html\n"
            "Then: export FRED_API_KEY=your_key"
        )
    return key


def fetch_series(
    series_id: str,
    api_key: str,
    observation_start: Optional[str] = None,
) -> list[dict]:
    """Fetch observations for a FRED series.

    Returns list of {"date": "YYYY-MM-DD", "value": float} dicts.
    """
    if observation_start is None:
        observation_start = (datetime.now() - timedelta(days=120)).strftime("%Y-%m-%d")

    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": observation_start,
        "sort_order": "desc",
    }

    resp = requests.get(FRED_BASE_URL, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    observations = []
    for obs in data.get("observations", []):
        if obs["value"] == ".":
            continue
        observations.append({
            "date": obs["date"],
            "value": float(obs["value"]),
        })

    return observations


def fetch_all(api_key: str, observation_start: Optional[str] = None) -> dict:
    """Fetch all tracked series. Returns {series_id: [observations]}."""
    results = {}
    for series_id in SERIES:
        results[series_id] = fetch_series(series_id, api_key, observation_start)
    return results
