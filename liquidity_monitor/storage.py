"""SQLite storage for historical liquidity data."""

from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "liquidity.db"


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS observations (
            series_id TEXT NOT NULL,
            date      TEXT NOT NULL,
            value     REAL NOT NULL,
            fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (series_id, date)
        )
    """)
    conn.commit()


def upsert_observations(conn: sqlite3.Connection, series_id: str, observations: list[dict]) -> int:
    """Insert or update observations. Returns number of new rows."""
    cursor = conn.cursor()
    inserted = 0
    for obs in observations:
        cursor.execute(
            """
            INSERT INTO observations (series_id, date, value)
            VALUES (?, ?, ?)
            ON CONFLICT (series_id, date) DO UPDATE SET
                value = excluded.value,
                fetched_at = datetime('now')
            """,
            (series_id, obs["date"], obs["value"]),
        )
        if cursor.rowcount > 0:
            inserted += 1
    conn.commit()
    return inserted


def get_latest(conn: sqlite3.Connection, series_id: str, limit: int = 1) -> list[dict]:
    """Get the most recent observations for a series."""
    rows = conn.execute(
        "SELECT date, value FROM observations WHERE series_id = ? ORDER BY date DESC LIMIT ?",
        (series_id, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def get_series_history(conn: sqlite3.Connection, series_id: str, days: int = 90) -> list[dict]:
    """Get historical observations ordered oldest-first."""
    rows = conn.execute(
        """
        SELECT date, value FROM observations
        WHERE series_id = ?
        ORDER BY date DESC LIMIT ?
        """,
        (series_id, days),
    ).fetchall()
    return [dict(r) for r in reversed(rows)]


def get_value_at_offset(conn: sqlite3.Connection, series_id: str, target_date: str) -> dict | None:
    """Get the closest observation on or before target_date."""
    row = conn.execute(
        """
        SELECT date, value FROM observations
        WHERE series_id = ? AND date <= ?
        ORDER BY date DESC LIMIT 1
        """,
        (series_id, target_date),
    ).fetchone()
    return dict(row) if row else None
