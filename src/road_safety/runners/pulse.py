"""Dynamic safety pulse: trend + forecast + long-term history snapshots."""

from __future__ import annotations

import json
import os
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

from ..data_access.utils import establish_connection


def fetch_monthly_totals(limit_months: int = 12) -> list[tuple[str, int]]:
    """Return monthly accident totals ordered chronologically."""
    conn = establish_connection()
    if not conn:
        raise RuntimeError(
            "Database connection failed. Check DB_HOST / DB_PORT / credentials."
        )

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TO_CHAR(date_acc, 'YYYY-MM') AS ym, COUNT(*)::int AS total
            FROM raw.accidents
            WHERE date_acc IS NOT NULL
            GROUP BY ym
            ORDER BY ym DESC
            LIMIT %s;
            """,
            (limit_months,),
        )
        rows = cur.fetchall()
        cur.close()
        return [(str(month), int(total)) for month, total in reversed(rows)]
    finally:
        conn.close()


def fetch_top_rising_communes(limit: int = 5) -> list[tuple[str, int, int, int]]:
    """Return communes with strongest increase in the latest 30d vs previous 30d."""
    conn = establish_connection()
    if not conn:
        raise RuntimeError(
            "Database connection failed. Check DB_HOST / DB_PORT / credentials."
        )

    try:
        cur = conn.cursor()
        cur.execute(
            """
            WITH latest AS (
                SELECT MAX(date_acc) AS max_date
                FROM raw.accidents
                WHERE date_acc IS NOT NULL
            ),
            recent AS (
                SELECT commune, COUNT(*)::int AS cnt
                FROM raw.accidents, latest
                WHERE date_acc > (latest.max_date - INTERVAL '30 days')
                  AND date_acc <= latest.max_date
                  AND commune IS NOT NULL
                GROUP BY commune
            ),
            previous AS (
                SELECT commune, COUNT(*)::int AS cnt
                FROM raw.accidents, latest
                WHERE date_acc > (latest.max_date - INTERVAL '60 days')
                  AND date_acc <= (latest.max_date - INTERVAL '30 days')
                  AND commune IS NOT NULL
                GROUP BY commune
            )
            SELECT
                COALESCE(r.commune, p.commune) AS commune,
                COALESCE(r.cnt, 0)::int AS recent_count,
                COALESCE(p.cnt, 0)::int AS previous_count,
                (COALESCE(r.cnt, 0) - COALESCE(p.cnt, 0))::int AS delta
            FROM recent r
            FULL OUTER JOIN previous p ON LOWER(r.commune) = LOWER(p.commune)
            ORDER BY delta DESC, recent_count DESC
            LIMIT %s;
            """,
            (limit,),
        )
        rows = cur.fetchall()
        cur.close()
        return [
            (str(commune), int(recent), int(previous), int(delta))
            for commune, recent, previous, delta in rows
        ]
    finally:
        conn.close()


def _sparkline(values: list[int]) -> str:
    """ASCII sparkline for quick trend visualization in terminal."""
    if not values:
        return "(no data)"

    if min(values) == max(values):
        return "=" * len(values)

    chars = " .:-=+*#%@"
    low = min(values)
    span = max(values) - low
    return "".join(chars[int((v - low) * (len(chars) - 1) / span)] for v in values)


def _forecast_next(values: list[int]) -> int:
    """Linear drift forecast for next point."""
    if not values:
        return 0
    if len(values) == 1:
        return values[0]

    slope = (values[-1] - values[0]) / (len(values) - 1)
    return max(0, int(round(values[-1] + slope)))


def build_snapshot(
    monthly: list[tuple[str, int]],
    rising: list[tuple[str, int, int, int]],
) -> dict[str, Any]:
    """Build one persisted pulse snapshot payload."""
    values = [value for _, value in monthly]
    latest_month = monthly[-1][0] if monthly else "n/a"
    latest_total = values[-1] if values else 0
    previous_total = values[-2] if len(values) > 1 else latest_total
    delta = latest_total - previous_total

    if delta > 0:
        trend = "up"
    elif delta < 0:
        trend = "down"
    else:
        trend = "stable"

    momentum_pct = 0.0
    if previous_total > 0:
        momentum_pct = (delta / previous_total) * 100.0

    return {
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "latest_month": latest_month,
        "latest_total": latest_total,
        "trend": trend,
        "delta_vs_previous_month": delta,
        "momentum_pct": round(momentum_pct, 2),
        "forecast_next_month": _forecast_next(values),
        "sparkline": _sparkline(values),
        "monthly": [{"month": m, "accidents": v} for m, v in monthly],
        "rising_communes": [
            {
                "commune": commune,
                "recent_count": recent,
                "previous_count": previous,
                "delta": delta_value,
            }
            for commune, recent, previous, delta_value in rising
        ],
    }


def load_history(history_path: str) -> list[dict[str, Any]]:
    """Read JSONL history, skipping malformed lines."""
    path = Path(history_path)
    if not path.exists():
        return []

    history: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw:
            continue
        try:
            history.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return history


def save_snapshot(history_path: str, snapshot: dict[str, Any]) -> None:
    """Append one snapshot as JSONL entry."""
    path = Path(history_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(snapshot, ensure_ascii=False) + "\n")


def compare_to_previous(
    history: list[dict[str, Any]], snapshot: dict[str, Any]
) -> dict[str, Any]:
    """Compute delta vs previous run (dynamic long-term signal)."""
    if not history:
        return {"run_index": 1, "delta_vs_previous_run": 0, "status": "baseline"}

    previous = history[-1]
    previous_total = int(previous.get("latest_total", 0))
    delta = int(snapshot["latest_total"]) - previous_total

    if delta > 0:
        status = "higher"
    elif delta < 0:
        status = "lower"
    else:
        status = "equal"

    return {
        "run_index": len(history) + 1,
        "delta_vs_previous_run": delta,
        "status": status,
    }


def render_pulse_report(
    snapshot: dict[str, Any], comparison: dict[str, Any], history_path: str
) -> str:
    """Render a terminal-friendly pulse report."""
    lines = [
        "",
        "ROAD SAFETY PULSE",
        "=================",
        f"Latest month        : {snapshot['latest_month']}",
        f"Accidents           : {snapshot['latest_total']}",
        (
            "Trend               : "
            f"{snapshot['trend']} ({snapshot['delta_vs_previous_month']:+d} vs previous month)"
        ),
        f"Momentum            : {snapshot['momentum_pct']:+.2f}%",
        f"Forecast next month : {snapshot['forecast_next_month']}",
        f"Monthly sparkline   : {snapshot['sparkline']}",
        (
            "Run comparison      : "
            f"{comparison['status']} ({comparison['delta_vs_previous_run']:+d} vs previous run)"
        ),
        "",
        "Top rising communes:",
    ]

    rising = snapshot.get("rising_communes", [])
    if not rising:
        lines.append("  - no rising commune found in the latest window")
    else:
        for row in rising:
            lines.append(
                "  - "
                f"{row['commune']}: {row['recent_count']} "
                f"(prev {row['previous_count']}, delta {row['delta']:+d})"
            )

    lines.extend(
        [
            "",
            f"History snapshot appended to: {os.path.abspath(history_path)}",
        ]
    )
    return "\n".join(lines)


def run_pulse(
    history_path: str = "src/road_safety/data/pulse_history.jsonl",
    months: int = 12,
    top: int = 5,
) -> None:
    """Entry point for `road-safety pulse`."""
    print(f"Building dynamic pulse (months={months}, top={top})…")
    try:
        monthly = fetch_monthly_totals(limit_months=months)
        rising = fetch_top_rising_communes(limit=top)
    except RuntimeError as exc:
        print(f"⚠️  {exc}")
        return

    snapshot = build_snapshot(monthly=monthly, rising=rising)
    history = load_history(history_path)
    comparison = compare_to_previous(history, snapshot)
    save_snapshot(history_path, snapshot)
    print(render_pulse_report(snapshot, comparison, history_path))
