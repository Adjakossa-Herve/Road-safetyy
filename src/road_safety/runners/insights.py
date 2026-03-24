"""Smart analytical insights for road safety data.

Provides a ``run_insights`` entry point that prints a summary of the most
dangerous patterns found in the accidents database (most dangerous hour,
weather condition, municipality, and intersection type).
"""

from __future__ import annotations

from typing import Any, Optional

from ..data_access.utils import establish_connection

FATAL_LABEL = "Tue"
SEVERE_LABEL = "Blessee hospitalisee"


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


def fetch_one(query: str, params: tuple = ()) -> Optional[tuple[Any, ...]]:
    """Return the first row of a SELECT query, or None."""
    conn = establish_connection()
    if not conn:
        raise RuntimeError(
            "Database connection failed. Check DB_HOST / DB_PORT / credentials."
        )
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        row = cur.fetchone()
        cur.close()
        return row
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Insight queries
# ---------------------------------------------------------------------------


def find_most_dangerous_hour() -> Optional[tuple[int, int]]:
    """Return (hour, accident_count) for the hour with the most accidents."""
    row = fetch_one(
        """
        SELECT
          EXTRACT(HOUR FROM NULLIF(heure_acc::text, '')::time)::int AS hour,
          COUNT(*)::int AS total
        FROM raw.accidents
        WHERE NULLIF(heure_acc::text, '') IS NOT NULL
        GROUP BY hour
        ORDER BY total DESC
        LIMIT 1;
        """
    )
    if row is None:
        return None
    hour, total = row
    return int(hour), int(total)


def find_most_dangerous_weather() -> Optional[tuple[str, int]]:
    """Return (cond_atmos, accident_count) for the most accident-prone weather."""
    row = fetch_one(
        """
        SELECT COALESCE(cond_atmos, 'UNKNOWN') AS cond_atmos, COUNT(*)::int AS total
        FROM raw.accidents
        GROUP BY cond_atmos
        ORDER BY total DESC
        LIMIT 1;
        """
    )
    if row is None:
        return None
    cond, total = row
    return str(cond), int(total)


def find_most_dangerous_commune() -> Optional[tuple[str, int]]:
    """Return (commune, accident_count) for the commune with the most accidents."""
    row = fetch_one(
        """
        SELECT COALESCE(commune, 'UNKNOWN') AS commune, COUNT(*)::int AS total
        FROM raw.accidents
        GROUP BY commune
        ORDER BY total DESC
        LIMIT 1;
        """
    )
    if row is None:
        return None
    commune, total = row
    return str(commune), int(total)


def find_most_dangerous_intersection() -> Optional[tuple[str, int]]:
    """Return (intersection, accident_count) for the most dangerous intersection type."""
    row = fetch_one(
        """
        SELECT COALESCE(intersection, 'UNKNOWN') AS intersection, COUNT(*)::int AS total
        FROM raw.accidents
        GROUP BY intersection
        ORDER BY total DESC
        LIMIT 1;
        """
    )
    if row is None:
        return None
    intersection, total = row
    return str(intersection), int(total)


def find_most_fatal_commune() -> Optional[tuple[str, int]]:
    """Return (commune, fatal_count) for the commune with the most fatal accidents."""
    row = fetch_one(
        """
        SELECT COALESCE(commune, 'UNKNOWN') AS commune, COUNT(*)::int AS fatalities
        FROM raw.accidents
        WHERE gravite_usager = %s
        GROUP BY commune
        ORDER BY fatalities DESC
        LIMIT 1;
        """,
        (FATAL_LABEL,),
    )
    if row is None:
        return None
    commune, total = row
    return str(commune), int(total)


# ---------------------------------------------------------------------------
# CLI display
# ---------------------------------------------------------------------------


def print_insights() -> None:
    """Fetch and display smart road-safety insights to stdout."""
    print("\n" + "=" * 50)
    print("   🚨  Road Safety Insights")
    print("=" * 50)

    hour_result = find_most_dangerous_hour()
    if hour_result:
        hour, count = hour_result
        print(f"  Most dangerous hour    : {hour:02d}:00  ({count} accidents)")
    else:
        print("  Most dangerous hour    : N/A")

    weather_result = find_most_dangerous_weather()
    if weather_result:
        cond, count = weather_result
        print(f"  Most dangerous weather : {cond}  ({count} accidents)")
    else:
        print("  Most dangerous weather : N/A")

    commune_result = find_most_dangerous_commune()
    if commune_result:
        commune, count = commune_result
        print(f"  Most dangerous commune : {commune}  ({count} accidents)")
    else:
        print("  Most dangerous commune : N/A")

    fatal_result = find_most_fatal_commune()
    if fatal_result:
        commune, count = fatal_result
        print(f"  Most fatal commune     : {commune}  ({count} fatalities)")
    else:
        print("  Most fatal commune     : N/A")

    intersection_result = find_most_dangerous_intersection()
    if intersection_result:
        inter, count = intersection_result
        print(f"  Most dangerous inter.  : {inter}  ({count} accidents)")
    else:
        print("  Most dangerous inter.  : N/A")

    print("=" * 50 + "\n")


def run_insights() -> None:
    """Entry point for the ``road-safety insights`` command."""
    print_insights()
