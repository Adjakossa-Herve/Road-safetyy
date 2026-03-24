from __future__ import annotations

from typing import Any

from road_safety.data_access.utils import establish_connection

FATAL_LABEL = "Tue"
SEVERE_LABEL = "Blessee hospitalisee"


def fetch_all(query: str, params: tuple = ()) -> list[tuple[Any, ...]]:
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


def group_count(column: str, limit: int = 20) -> list[tuple[str, int]]:
    rows = fetch_all(
        f"""
        SELECT COALESCE({column}, 'UNKNOWN') AS key, COUNT(*)::int AS total
        FROM raw.accidents
        GROUP BY key
        ORDER BY total DESC
        LIMIT %s;
        """,
        (limit,),
    )
    return [(str(k), int(v)) for k, v in rows]


def severity_by_gender() -> list[tuple[str, int, int, int]]:
    rows = fetch_all(
        """
        SELECT
          COALESCE(sexe_usager, 'UNKNOWN') AS sexe,
          COUNT(*)::int AS total,
          SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int AS fatalities,
          SUM(CASE WHEN gravite_usager IN (%s, %s) THEN 1 ELSE 0 END)::int AS severe
        FROM raw.accidents
        GROUP BY sexe
        ORDER BY total DESC;
        """,
        (FATAL_LABEL, FATAL_LABEL, SEVERE_LABEL),
    )
    return [(str(s), int(t), int(f), int(sev)) for s, t, f, sev in rows]


def severity_by_age_group() -> list[tuple[str, int, int, int]]:
    rows = fetch_all(
        """
        SELECT
          CASE
            WHEN age_usager IS NULL THEN 'UNKNOWN'
            WHEN age_usager < 18 THEN '<18'
            WHEN age_usager BETWEEN 18 AND 24 THEN '18-24'
            WHEN age_usager BETWEEN 25 AND 34 THEN '25-34'
            WHEN age_usager BETWEEN 35 AND 44 THEN '35-44'
            WHEN age_usager BETWEEN 45 AND 54 THEN '45-54'
            WHEN age_usager BETWEEN 55 AND 64 THEN '55-64'
            WHEN age_usager BETWEEN 65 AND 74 THEN '65-74'
            ELSE '75+'
          END AS age_group,
          COUNT(*)::int AS total,
          SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int AS fatalities,
          SUM(CASE WHEN gravite_usager IN (%s, %s) THEN 1 ELSE 0 END)::int AS severe
        FROM raw.accidents
        GROUP BY age_group
        ORDER BY total DESC;
        """,
        (FATAL_LABEL, FATAL_LABEL, SEVERE_LABEL),
    )
    return [(str(g), int(t), int(f), int(sev)) for g, t, f, sev in rows]


def pedestrians_vs_vehicles() -> tuple[int, int, int, int]:
    rows = fetch_all(
        """
        SELECT
          SUM(COALESCE(nombre_pietons, 0))::int AS pedestrians,
          SUM(COALESCE(nombre_motos, 0))::int AS motos,
          SUM(COALESCE(nombre_vl, 0))::int AS vl,
          SUM(COALESCE(nombre_pl, 0))::int AS pl
        FROM raw.accidents;
        """
    )
    if not rows:
        return (0, 0, 0, 0)

    pedestrians, motos, vl, pl = rows[0]
    return (int(pedestrians or 0), int(motos or 0), int(vl or 0), int(pl or 0))
