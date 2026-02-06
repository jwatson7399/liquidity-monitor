#!/usr/bin/env python3
"""US Liquidity Monitor — daily terminal dashboard."""

import argparse
import sys

from liquidity_monitor.fred_client import fetch_all, get_api_key, SERIES
from liquidity_monitor.storage import get_connection, init_db, upsert_observations
from liquidity_monitor.metrics import get_current_snapshot, get_net_liquidity_history
from liquidity_monitor.report import render_report


def cmd_fetch(args):
    """Fetch latest data from FRED and store it."""
    api_key = get_api_key()
    conn = get_connection()
    init_db(conn)

    print("Fetching from FRED API...")
    all_data = fetch_all(api_key)

    total = 0
    for series_id, observations in all_data.items():
        n = upsert_observations(conn, series_id, observations)
        label = SERIES.get(series_id, series_id)
        print(f"  {label} ({series_id}): {len(observations)} observations, {n} upserted")
        total += n

    print(f"Done. {total} total rows upserted.")
    conn.close()


def cmd_report(args):
    """Generate the terminal report from stored data."""
    conn = get_connection()
    init_db(conn)

    snapshot = get_current_snapshot(conn)
    history = get_net_liquidity_history(conn, days=90)

    if not any(v.get("current") is not None for v in snapshot.values()):
        print("No data found. Run `python main.py fetch` first.")
        sys.exit(1)

    render_report(snapshot, history)
    conn.close()


def cmd_run(args):
    """Fetch + report in one step."""
    cmd_fetch(args)
    print()
    cmd_report(args)


def cmd_serve(args):
    """Start the web dashboard server."""
    from liquidity_monitor.web import create_app
    app = create_app()
    print(f"Starting dashboard at http://{args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=args.debug)


def main():
    parser = argparse.ArgumentParser(description="US Liquidity Monitor")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("fetch", help="Fetch latest data from FRED")
    sub.add_parser("report", help="Show report from stored data")
    sub.add_parser("run", help="Fetch data then show report")

    serve_parser = sub.add_parser("serve", help="Start web dashboard")
    serve_parser.add_argument("--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
    serve_parser.add_argument("--port", type=int, default=5050, help="Port (default: 5050)")
    serve_parser.add_argument("--debug", action="store_true", help="Enable debug mode")

    args = parser.parse_args()

    commands = {
        "fetch": cmd_fetch,
        "report": cmd_report,
        "run": cmd_run,
        "serve": cmd_serve,
    }

    if args.command in commands:
        commands[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
