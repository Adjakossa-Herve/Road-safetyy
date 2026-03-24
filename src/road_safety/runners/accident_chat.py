import re
from typing import Any, Iterable, Optional, Sequence

from ..data_access.utils import establish_connection

# Optional menu import (must not break tests)
try:
    from .accident_cli import run_menu
except Exception:  # pragma: no cover
    run_menu = None


# Severity labels (must match your cleaned values)
FATAL_LABEL = "Tue"
SEVERE_LABEL = "Blessee hospitalisee"
LIGHT_LABEL = "Blessee Leger"

HELP_TEXT = """
Road Safety interactive CLI

General:
  help
  exit
  menu                          -> open menu (optional)

Overview / severity:
  overview                       -> severity breakdown + total
  fatal_rate                     -> fatal proportion
  collisions                     -> most frequent collision types
  gravity_values 20              -> show distinct gravite_usager values

Time:
  by_hour                        -> accidents per hour
  day_vs_night                   -> stats grouped by luminosite
  by_month                       -> accidents per month (YYYY-MM)
  weekend_vs_week                -> compare weekend vs week (counts + fatal/severe)

Location:
  top_communes 10                -> top communes by total accidents
  stats commune Paris            -> KPIs for a commune (total, fatal, severe)

Extended analytics:
  top_fatal_communes 10
  top_severe_communes 10
  risk_score_communes 10
  risk_score commune Paris
  trend_days 2026-01-01 2026-01-31
  trend_days 2026-01-01 2026-01-31 commune Paris

Urgency dashboard:
  stats_urgences                 -> operational statistics for emergency services
"""


# ---------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------
def fetch_all(query: str, params: tuple = ()) -> list[tuple[Any, ...]]:
    """Fetch rows for a SELECT query."""
    conn = establish_connection()
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


def print_kv(title: str, rows: Iterable[tuple[Any, Any]]) -> None:
    """Print key/value rows."""
    print(title)
    for k, v in rows:
        print(f"- {k}: {v}")


# ---------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------
def fetch_table_columns(schema: str, table: str) -> list[tuple[str, str]]:
    return fetch_all(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;
        """,
        (schema, table),
    )


# ---------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------
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


def list_gravity_values(limit: int = 50) -> list[tuple[str, int]]:
    rows = fetch_all(
        """
        SELECT COALESCE(gravite_usager, 'NULL') AS gravite, COUNT(*)::int AS total
        FROM raw.accidents
        GROUP BY gravite
        ORDER BY total DESC
        LIMIT %s;
        """,
        (limit,),
    )
    return [(str(g), int(t)) for g, t in rows]


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


def compute_commune_kpis(commune: str) -> tuple[int, int, int]:
    rows = fetch_all(
        """
        SELECT
          COUNT(*)::int AS total,
          SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int AS fatalities,
          SUM(CASE WHEN gravite_usager IN (%s, %s) THEN 1 ELSE 0 END)::int AS severe
        FROM raw.accidents
        WHERE LOWER(commune) = LOWER(%s);
        """,
        (FATAL_LABEL, FATAL_LABEL, SEVERE_LABEL, commune),
    )
    total, fatalities, severe = rows[0]
    return int(total or 0), int(fatalities or 0), int(severe or 0)


def compute_hourly_distribution() -> list[tuple[int, int]]:
    rows = fetch_all(
        """
        SELECT
          EXTRACT(HOUR FROM NULLIF(heure_acc::text, '')::time)::int AS hour,
          COUNT(*)::int AS total
        FROM raw.accidents
        WHERE NULLIF(heure_acc::text, '') IS NOT NULL
        GROUP BY hour
        ORDER BY total DESC;
        """
    )
    return [(int(h), int(t)) for h, t in rows if h is not None]


def compute_day_vs_night_stats() -> list[tuple[str, int, int, int]]:
    rows = fetch_all(
        """
        SELECT COALESCE(luminosite, 'UNKNOWN') AS luminosite,
               COUNT(*)::int AS total,
               SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int AS fatalities,
               SUM(CASE WHEN gravite_usager IN (%s, %s) THEN 1 ELSE 0 END)::int AS severe
        FROM raw.accidents
        GROUP BY luminosite
        ORDER BY total DESC;
        """,
        (FATAL_LABEL, FATAL_LABEL, SEVERE_LABEL),
    )
    return [(str(l), int(t), int(f), int(s)) for l, t, f, s in rows]


def compute_monthly_distribution() -> list[tuple[str, int]]:
    rows = fetch_all(
        """
        SELECT TO_CHAR(date_acc, 'YYYY-MM') AS month, COUNT(*)::int AS total
        FROM raw.accidents
        WHERE date_acc IS NOT NULL
        GROUP BY month
        ORDER BY month;
        """
    )
    return [(str(m), int(t)) for m, t in rows]


def compute_weekend_severity_gap() -> list[tuple[str, int, int, int]]:
    rows = fetch_all(
        """
        SELECT
          CASE WHEN EXTRACT(DOW FROM date_acc) IN (0,6) THEN 'WEEKEND' ELSE 'WEEKDAY' END AS period,
          COUNT(*)::int AS total,
          SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int AS fatalities,
          SUM(CASE WHEN gravite_usager IN (%s, %s) THEN 1 ELSE 0 END)::int AS severe
        FROM raw.accidents
        WHERE date_acc IS NOT NULL
        GROUP BY period
        ORDER BY period;
        """,
        (FATAL_LABEL, FATAL_LABEL, SEVERE_LABEL),
    )
    return [(str(p), int(t), int(f), int(s)) for p, t, f, s in rows]


# ---------------- Extended analytics (flagged) ----------------
def list_top_fatal_communes(limit: int = 10) -> list[tuple[str, int]]:
    rows = fetch_all(
        """
        SELECT COALESCE(commune, 'UNKNOWN') AS commune, COUNT(*)::int AS fatalities
        FROM raw.accidents
        WHERE gravite_usager = %s
        GROUP BY commune
        ORDER BY fatalities DESC
        LIMIT %s;
        """,
        (FATAL_LABEL, limit),
    )
    return [(str(c), int(t)) for c, t in rows]


def list_top_severe_communes(limit: int = 10) -> list[tuple[str, int]]:
    rows = fetch_all(
        """
        SELECT COALESCE(commune, 'UNKNOWN') AS commune, COUNT(*)::int AS severe_accidents
        FROM raw.accidents
        WHERE gravite_usager IN (%s, %s)
        GROUP BY commune
        ORDER BY severe_accidents DESC
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
          SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int AS fatalities,
          SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int AS severe,
          SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int AS light,
          (
            3 * SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)
            + 2 * SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)
            + 1 * SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)
          )::int AS risk_score
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
          SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int AS fatalities,
          SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int AS severe,
          SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int AS light,
          (
            3 * SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)
            + 2 * SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)
            + 1 * SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)
          )::int AS risk_score
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
            SELECT date_acc::date::text AS day, COUNT(*)::int AS total
            FROM raw.accidents
            WHERE date_acc BETWEEN %s AND %s
              AND LOWER(commune) = LOWER(%s)
            GROUP BY day
            ORDER BY day;
            """,
            (date_from, date_to, commune),
        )
    else:
        rows = fetch_all(
            """
            SELECT date_acc::date::text AS day, COUNT(*)::int AS total
            FROM raw.accidents
            WHERE date_acc BETWEEN %s AND %s
            GROUP BY day
            ORDER BY day;
            """,
            (date_from, date_to),
        )
    return [(str(d), int(t)) for d, t in rows]


# ---------------------------------------------------------------------
# Command wrappers (q_*)
# ---------------------------------------------------------------------
def q_overview() -> None:
    print_table(["gravite_usager", "total"], compute_severity_breakdown())


def q_fatal_rate() -> None:
    rate, fatalities, total = compute_fatal_rate()
    print_table(["fatal_rate_%", "fatalities", "total"], [(rate, fatalities, total)])


def q_collisions() -> None:
    print_table(["type_collision", "total"], list_collision_types())


def q_gravity_values(limit: int) -> None:
    print_table(["gravite_usager", "total"], list_gravity_values(limit))


def q_by_hour() -> None:
    print_table(["hour", "total"], compute_hourly_distribution())


def q_day_vs_night() -> None:
    print_table(
        ["luminosite", "total", "fatalities", "severe"], compute_day_vs_night_stats()
    )


def q_by_month() -> None:
    print_table(["month", "total"], compute_monthly_distribution())


def q_weekend_vs_week() -> None:
    print_table(
        ["period", "total", "fatalities", "severe"], compute_weekend_severity_gap()
    )


def q_top_communes(limit: int) -> None:
    print_table(["commune", "total"], list_top_communes(limit))


def q_stats_commune(commune: str) -> None:
    total, fatalities, severe = compute_commune_kpis(commune)
    print_table(
        ["commune", "total", "fatalities", "severe"],
        [(commune, total, fatalities, severe)],
    )


# Extended wrappers
def q_top_fatal_communes(limit: int) -> None:
    print_table(["commune", "fatalities"], list_top_fatal_communes(limit))


def q_top_severe_communes(limit: int) -> None:
    print_table(["commune", "severe_accidents"], list_top_severe_communes(limit))


def q_risk_score_communes(limit: int) -> None:
    print_table(
        ["commune", "fatalities", "severe", "light", "risk_score"],
        compute_risk_score_by_commune(limit),
    )


def q_risk_score_commune(commune: str) -> None:
    f, s, l, rs = compute_commune_risk_score(commune)
    print_table(
        ["commune", "fatalities", "severe", "light", "risk_score"],
        [(commune, f, s, l, rs)],
    )


def q_trend_days(date_from: str, date_to: str, commune: Optional[str]) -> None:
    print_table(["day", "total"], compute_trend_days(date_from, date_to, commune))


def q_columns(schema: str, table: str) -> None:
    print_table(["column_name", "data_type"], fetch_table_columns(schema, table))


def fetch_operational_stats() -> None:
    """Affiche les statistiques opérationnelles pour les services d'urgence."""
    section = lambda title: print(f"\n{'=' * 52}\n  {title}\n{'=' * 52}")

    section("Types d'accidents")
    rows = fetch_all("""
        SELECT COALESCE(type_acci, 'Unknown') AS type_acci, COUNT(*)::int AS total
        FROM raw.accidents GROUP BY type_acci ORDER BY total DESC LIMIT 10;
    """)
    print_table(["type_acci", "total"], rows)

    section("Conditions météo")
    rows = fetch_all("""
        SELECT COALESCE(cond_atmos, 'Unknown') AS cond_atmos, COUNT(*)::int AS total
        FROM raw.accidents GROUP BY cond_atmos ORDER BY total DESC;
    """)
    print_table(["cond_atmos", "total"], rows)

    section("Luminosité")
    rows = fetch_all("""
        SELECT COALESCE(luminosite, 'Unknown') AS luminosite, COUNT(*)::int AS total
        FROM raw.accidents GROUP BY luminosite ORDER BY total DESC;
    """)
    print_table(["luminosite", "total"], rows)

    section("Intersections")
    rows = fetch_all("""
        SELECT COALESCE(intersection, 'Unknown') AS intersection, COUNT(*)::int AS total
        FROM raw.accidents GROUP BY intersection ORDER BY total DESC;
    """)
    print_table(["intersection", "total"], rows)

    section("Catégories de route")
    rows = fetch_all("""
        SELECT COALESCE(categorie_route, 'Unknown') AS categorie_route, COUNT(*)::int AS total
        FROM raw.accidents GROUP BY categorie_route ORDER BY total DESC;
    """)
    print_table(["categorie_route", "total"], rows)

    section("Types de collision")
    rows = fetch_all("""
        SELECT COALESCE(type_collision, 'Unknown') AS type_collision, COUNT(*)::int AS total
        FROM raw.accidents GROUP BY type_collision ORDER BY total DESC;
    """)
    print_table(["type_collision", "total"], rows)

    section("Véhicules impliqués (type 1)")
    rows = fetch_all("""
        SELECT COALESCE(type_vehicule_1, 'Unknown') AS type_vehicule_1, COUNT(*)::int AS total
        FROM raw.accidents GROUP BY type_vehicule_1 ORDER BY total DESC LIMIT 10;
    """)
    print_table(["type_vehicule_1", "total"], rows)

    section("Heures les plus dangereuses (Top 5)")
    rows = fetch_all("""
        SELECT EXTRACT(HOUR FROM NULLIF(heure_acc::text,'')::time)::int AS heure,
               COUNT(*)::int AS total
        FROM raw.accidents WHERE NULLIF(heure_acc::text,'') IS NOT NULL
        GROUP BY heure ORDER BY total DESC LIMIT 5;
    """)
    print_table(["heure", "total"], rows)

    section("Taux de mortalité par commune (Top 10)")
    rows = fetch_all("""
        SELECT commune,
               SUM(CASE WHEN gravite_usager = 'Tue' THEN 1 ELSE 0 END)::int AS deces,
               COUNT(*)::int AS accidents,
               ROUND(SUM(CASE WHEN gravite_usager = 'Tue' THEN 1 ELSE 0 END)::numeric
                     / NULLIF(COUNT(*), 0) * 100, 2) AS taux_mortalite_pct
        FROM raw.accidents
        GROUP BY commune
        ORDER BY taux_mortalite_pct DESC NULLS LAST
        LIMIT 10;
    """)
    print_table(["commune", "deces", "accidents", "taux_mortalite_%"], rows)

    section("Moyennes opérationnelles")
    rows = fetch_all("""
        SELECT
            ROUND(AVG(nombre_usagers)::numeric, 2) AS avg_usagers,
            ROUND(AVG(nb_veh)::numeric, 2)         AS avg_vehicules,
            ROUND(AVG(age_usager)::numeric, 1)     AS avg_age,
            ROUND(AVG(nombre_pietons)::numeric, 2) AS avg_pietons,
            ROUND(AVG(nombre_motos)::numeric, 2)   AS avg_motos
        FROM raw.accidents;
    """)
    if rows:
        r = rows[0]
        print(f"  Usagers/accident   : {r[0]}")
        print(f"  Véhicules/accident : {r[1]}")
        print(f"  Âge moyen usager   : {r[2]}")
        print(f"  Piétons/accident   : {r[3]}")
        print(f"  Motos/accident     : {r[4]}")


def q_stats_urgences() -> None:
    fetch_operational_stats()


# ---------------------------------------------------------------------
# REPL (tests expect this behaviour)
# ---------------------------------------------------------------------
def run_chat() -> None:
    print("=== Road Safety Interactive ===")
    print("Type 'help' for commands, 'exit' to quit.")

    while True:
        q = input("\n> ").strip()
        if not q:
            continue

        low = q.lower()

        if low in {"exit", "quit"}:
            print("Bye.")
            return

        if low in {"help", "h", "?"}:
            print(HELP_TEXT)
            continue

        if low == "menu" and run_menu is not None:
            run_menu()
            continue

        # Fixed commands
        if low == "overview":
            q_overview()
            continue
        if low == "fatal_rate":
            q_fatal_rate()
            continue
        if low == "collisions":
            q_collisions()
            continue
        if low == "by_hour":
            q_by_hour()
            continue
        if low == "day_vs_night":
            q_day_vs_night()
            continue
        if low == "by_month":
            q_by_month()
            continue
        if low == "weekend_vs_week":
            q_weekend_vs_week()
            continue

        if low == "stats_urgences":
            q_stats_urgences()
            continue

        # Parameterized basics
        m = re.match(r"^top_communes\s+(\d+)$", q, re.IGNORECASE)
        if m:
            q_top_communes(int(m.group(1)))
            continue

        m = re.match(r"^stats\s+commune\s+(.+)$", q, re.IGNORECASE)
        if m:
            q_stats_commune(m.group(1).strip())
            continue

        m = re.match(r"^gravity_values\s+(\d+)$", q, re.IGNORECASE)
        if m:
            q_gravity_values(int(m.group(1)))
            continue

        m = re.match(
            r"^columns\s+([a-zA-Z_][\w]*)\s+([a-zA-Z_][\w]*)$", q, re.IGNORECASE
        )
        if m:
            q_columns(m.group(1), m.group(2))
            continue

        # Extended analytics
        m = re.match(r"^top_fatal_communes\s+(\d+)$", q, re.IGNORECASE)
        if m:
            q_top_fatal_communes(int(m.group(1)))
            continue

        m = re.match(r"^top_severe_communes\s+(\d+)$", q, re.IGNORECASE)
        if m:
            q_top_severe_communes(int(m.group(1)))
            continue

        m = re.match(r"^risk_score_communes\s+(\d+)$", q, re.IGNORECASE)
        if m:
            q_risk_score_communes(int(m.group(1)))
            continue

        m = re.match(r"^risk_score\s+commune\s+(.+)$", q, re.IGNORECASE)
        if m:
            q_risk_score_commune(m.group(1).strip())
            continue

        m = re.match(
            r"^trend_days\s+(\d{4}-\d{2}-\d{2})\s+(\d{4}-\d{2}-\d{2})(?:\s+commune\s+(.+))?$",
            q,
            re.IGNORECASE,
        )
        if m:
            date_from = m.group(1)
            date_to = m.group(2)
            commune = m.group(3).strip() if m.group(3) else None
            q_trend_days(date_from, date_to, commune)
            continue

        print("Unknown command. Type 'help' to see available commands.")
