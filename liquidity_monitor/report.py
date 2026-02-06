"""Terminal report generation using Rich."""

from __future__ import annotations

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

from .fred_client import SERIES

# Display order
DISPLAY_ORDER = ["WALCL", "WRESBAL", "M2SL", "WTREGEN", "RRPONTSYD", "NET_LIQUIDITY"]

# Units: RRPONTSYD is billions, rest are millions
UNITS = {
    "WALCL": ("M", 1e6),       # millions -> trillions display
    "WRESBAL": ("M", 1e6),
    "M2SL": ("B", 1e3),        # billions -> trillions display
    "WTREGEN": ("M", 1e6),
    "RRPONTSYD": ("B", 1e3),
    "NET_LIQUIDITY": ("M", 1e6),
}

SPARK_CHARS = "▁▂▃▄▅▆▇█"


def sparkline(values: list[float], width: int = 30) -> str:
    if not values:
        return ""
    # Sample down to width points
    if len(values) > width:
        step = len(values) / width
        sampled = [values[int(i * step)] for i in range(width)]
    else:
        sampled = values

    lo, hi = min(sampled), max(sampled)
    spread = hi - lo if hi != lo else 1.0
    return "".join(
        SPARK_CHARS[min(int((v - lo) / spread * (len(SPARK_CHARS) - 1)), len(SPARK_CHARS) - 1)]
        for v in sampled
    )


def fmt_trillions(value: float | None, divisor: float) -> str:
    if value is None:
        return "—"
    return f"${value / divisor:.3f}T"


def fmt_change(value: float | None, divisor: float) -> Text:
    if value is None:
        return Text("—", style="dim")
    billions = value / (divisor / 1000)
    sign = "+" if value >= 0 else ""
    color = "green" if value >= 0 else "red"
    return Text(f"{sign}{billions:.1f}B", style=color)


def render_report(snapshot: dict, net_liquidity_history: list[dict]) -> None:
    console = Console()

    # Header
    console.print()
    console.print(
        Panel(
            "[bold]US Liquidity Monitor[/bold]",
            subtitle="Source: FRED / St. Louis Fed",
            style="blue",
        )
    )

    # Main data table
    table = Table(show_header=True, header_style="bold cyan", padding=(0, 1))
    table.add_column("Metric", style="bold", min_width=24)
    table.add_column("Latest", justify="right", min_width=12)
    table.add_column("As Of", justify="center", min_width=12)
    table.add_column("1W Chg", justify="right", min_width=10)
    table.add_column("1M Chg", justify="right", min_width=10)

    for sid in DISPLAY_ORDER:
        entry = snapshot.get(sid)
        if not entry or entry.get("current") is None:
            continue

        _, divisor = UNITS.get(sid, ("M", 1e6))

        if sid == "NET_LIQUIDITY":
            table.add_section()

        table.add_row(
            entry["label"],
            fmt_trillions(entry["current"], divisor),
            entry.get("current_date", "—"),
            fmt_change(entry.get("week_change"), divisor),
            fmt_change(entry.get("month_change"), divisor),
        )

    console.print(table)

    # Net liquidity sparkline
    if net_liquidity_history:
        values = [p["value"] for p in net_liquidity_history]
        spark = sparkline(values)
        lo = min(values)
        hi = max(values)
        dates_range = f"{net_liquidity_history[0]['date']} → {net_liquidity_history[-1]['date']}"

        console.print()
        console.print(
            Panel(
                f"[cyan]{spark}[/cyan]\n"
                f"[dim]{dates_range}[/dim]\n"
                f"[dim]Range: ${lo/1e6:.3f}T – ${hi/1e6:.3f}T[/dim]",
                title="[bold]Net Liquidity Trend[/bold]",
                style="blue",
            )
        )

    console.print()
