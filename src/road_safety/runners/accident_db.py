import os
from typing import Any, Callable, Optional, Sequence

from ..data_access.utils import establish_connection as _establish_connection

# Severity labels (can be overridden by environment variables if needed)
FATAL_LABEL = os.getenv("FATAL_LABEL", "Tue")
SEVERE_LABEL = os.getenv("SEVERE_LABEL", "Blessee hospitalisee")
LIGHT_LABEL = os.getenv("LIGHT_LABEL", "Blessee Leger")


def fetch_all(
    query: str,
    params: tuple = (),
    connect_func: Callable[[], Any] = _establish_connection,
) -> list[tuple[Any, ...]]:
    """Fetch rows for a SELECT query."""
    conn = connect_func()
    if not conn:
        raise RuntimeError(
            "Database connection failed. Check DB_HOST / DB_PORT / credentials."
        )
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        conn.close()


def print_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
    """Print rows as a simple ASCII table."""
    if not rows:
        print("(no results)")
        return

    widths = [len(h) for h in headers]
    for r in rows:
        for i, v in enumerate(r):
            widths[i] = max(widths[i], len(str(v)))

    def fmt(vals: Sequence[Any]) -> str:
        return " | ".join(str(vals[i]).ljust(widths[i]) for i in range(len(headers)))

    print(fmt(headers))
    print("-+-".join("-" * w for w in widths))
    for r in rows:
        print(fmt(r))


def fetch_table_columns(
    schema: str,
    table: str,
    connect_func=_establish_connection,
) -> list[tuple[str, str]]:
    return fetch_all(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;
        """,
        (schema, table),
        connect_func=connect_func,
    )


def compute_severity_breakdown() -> list[tuple[str, int]]:
    rows = fetch_all(
        """
        SELECT COALESCE(gravite_usager, 'UNKNOWN') AS gravite, COUNT(*)::int AS total
        FROM raw.accidents
        GROUP BY gravite
        ORDER BY total DESC;
        """
    )
    return [(str(g), int(t)) for g, t in rows]


def compute_fatal_rate() -> tuple[float, int, int]:
    rows = fetch_all(
        """
        SELECT
          ROUND(
            (SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END) * 100.0) / NULLIF(COUNT(*), 0),
            3
          ) AS fatal_rate_percent,
          SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int AS fatalities,
          COUNT(*)::int AS total
        FROM raw.accidents;
        """,
        (FATAL_LABEL, FATAL_LABEL),
    )
    fatal_rate_percent, fatalities, total = rows[0]
    return float(fatal_rate_percent or 0.0), int(fatalities or 0), int(total or 0)


def list_collision_types() -> list[tuple[str, int]]:
    rows = fetch_all(
        """
        SELECT COALESCE(type_collision, 'UNKNOWN') AS type_collision, COUNT(*)::int AS total
        FROM raw.accidents
        GROUP BY type_collision
        ORDER BY total DESC;
        """
    )
    return [(str(tc), int(t)) for tc, t in rows]


def list_top_communes(limit: int = 10) -> list[tuple[str, int]]:
    rows = fetch_all(
        """
        SELECT COALESCE(commune, 'UNKNOWN') AS commune, COUNT(*)::int AS total
        FROM raw.accidents
        GROUP BY commune
        ORDER BY total DESC
        LIMIT %s;
        """,
        (limit,),
    )
    return [(str(c), int(t)) for c, t in rows]


def list_top_fatal_communes(limit: int = 10) -> list[tuple[str, int]]:
    rows = fetch_all(
        """
        SELECT COALESCE(commune, 'UNKNOWN') AS commune,
               COUNT(*)::int AS deces
        FROM raw.accidents
        WHERE gravite_usager = %s
        GROUP BY commune
        ORDER BY deces DESC
        LIMIT %s;
        """,
        (FATAL_LABEL, limit),
    )
    return [(str(c), int(t)) for c, t in rows]


def list_top_severe_communes(limit: int = 10) -> list[tuple[str, int]]:
    rows = fetch_all(
        """
        SELECT COALESCE(commune, 'UNKNOWN') AS commune,
               COUNT(*)::int AS accidents_graves
        FROM raw.accidents
        WHERE gravite_usager IN (%s, %s)
        GROUP BY commune
        ORDER BY accidents_graves DESC
        LIMIT %s;
        """,
        (FATAL_LABEL, SEVERE_LABEL, limit),
    )
    return [(str(c), int(t)) for c, t in rows]


def compute_risk_score_by_commune(
    limit: int = 10,
) -> list[tuple[str, int, int, int, int]]:
    rows = fetch_all(
        """
        SELECT
          COALESCE(commune, 'UNKNOWN') AS commune,
          SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int AS deces,
          SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int AS grave,
          SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int AS leger,
          (3 * SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)
           + 2 * SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)
           + 1 * SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END))::int AS risk_score
        FROM raw.accidents
        GROUP BY commune
        ORDER BY risk_score DESC
        LIMIT %s;
        """,
        (
            FATAL_LABEL,
            SEVERE_LABEL,
            LIGHT_LABEL,
            FATAL_LABEL,
            SEVERE_LABEL,
            LIGHT_LABEL,
            limit,
        ),
    )
    return [(str(c), int(f), int(s), int(l), int(rs)) for c, f, s, l, rs in rows]


def compute_commune_risk_score(commune: str) -> tuple[int, int, int, int]:
    rows = fetch_all(
        """
        SELECT
          SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int,
          SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int,
          SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int,
          (3 * SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)
           + 2 * SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)
           + 1 * SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END))::int
        FROM raw.accidents
        WHERE LOWER(commune) = LOWER(%s);
        """,
        (
            FATAL_LABEL,
            SEVERE_LABEL,
            LIGHT_LABEL,
            FATAL_LABEL,
            SEVERE_LABEL,
            LIGHT_LABEL,
            commune,
        ),
    )
    f, s, l, rs = rows[0]
    return int(f or 0), int(s or 0), int(l or 0), int(rs or 0)


def compute_trend_days(
    date_from: str, date_to: str, commune: Optional[str] = None
) -> list[tuple[str, int]]:
    if commune:
        rows = fetch_all(
            """
            SELECT date_acc::date::text AS jour, COUNT(*)::int AS total
            FROM raw.accidents
            WHERE date_acc BETWEEN %s AND %s
              AND LOWER(commune) = LOWER(%s)
            GROUP BY jour ORDER BY jour;
            """,
            (date_from, date_to, commune),
        )
    else:
        rows = fetch_all(
            """
            SELECT date_acc::date::text AS jour, COUNT(*)::int AS total
            FROM raw.accidents
            WHERE date_acc BETWEEN %s AND %s
            GROUP BY jour ORDER BY jour;
            """,
            (date_from, date_to),
        )
    return [(str(d), int(t)) for d, t in rows]
