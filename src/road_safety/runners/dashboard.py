"""Streamlit analytical dashboard for road safety data.

This module exposes a ``run_dashboard`` entry point that launches a Streamlit
web application.  It also contains the pure-data functions that the dashboard
uses so that they can be unit-tested independently of Streamlit.

Run from the CLI via:
    road-safety dashboard

Or directly:
    streamlit run src/road_safety/runners/dashboard.py
"""

from __future__ import annotations

import subprocess
import sys
from typing import Any


# ---------------------------------------------------------------------------
# Data-layer helpers (testable without Streamlit)
# ---------------------------------------------------------------------------


def _fetch_all(query: str, params: tuple = ()) -> list[tuple[Any, ...]]:
    """Minimal DB helper – avoids importing the full utils at module level."""
    from ..data_access.utils import establish_connection

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


def fetch_accidents_by_year() -> list[tuple[str, int]]:
    """Return (year, count) pairs ordered by year."""
    rows = _fetch_all(
        """
        SELECT TO_CHAR(date_acc, 'YYYY') AS year, COUNT(*)::int AS total
        FROM raw.accidents
        WHERE date_acc IS NOT NULL
        GROUP BY year
        ORDER BY year;
        """
    )
    return [(str(y), int(t)) for y, t in rows]


def fetch_accidents_by_commune(limit: int = 15) -> list[tuple[str, int]]:
    """Return (commune, count) pairs for the top *limit* communes."""
    rows = _fetch_all(
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


def fetch_accidents_by_hour() -> list[tuple[int, int]]:
    """Return (hour, count) pairs ordered by hour."""
    rows = _fetch_all(
        """
        SELECT
          EXTRACT(HOUR FROM NULLIF(heure_acc::text, '')::time)::int AS hour,
          COUNT(*)::int AS total
        FROM raw.accidents
        WHERE NULLIF(heure_acc::text, '') IS NOT NULL
        GROUP BY hour
        ORDER BY hour;
        """
    )
    return [(int(h), int(t)) for h, t in rows if h is not None]


def fetch_accidents_by_weather() -> list[tuple[str, int]]:
    """Return (cond_atmos, count) pairs ordered by count desc."""
    rows = _fetch_all(
        """
        SELECT COALESCE(cond_atmos, 'UNKNOWN') AS cond, COUNT(*)::int AS total
        FROM raw.accidents
        GROUP BY cond
        ORDER BY total DESC;
        """
    )
    return [(str(c), int(t)) for c, t in rows]


def fetch_severity_distribution() -> list[tuple[str, int]]:
    """Return (severity_label, count) pairs ordered by count desc."""
    rows = _fetch_all(
        """
        SELECT COALESCE(gravite_usager, 'UNKNOWN') AS gravite, COUNT(*)::int AS total
        FROM raw.accidents
        GROUP BY gravite
        ORDER BY total DESC;
        """
    )
    return [(str(g), int(t)) for g, t in rows]


# ---------------------------------------------------------------------------
# Streamlit page (only runs when Streamlit imports this file)
# ---------------------------------------------------------------------------


def _render_dashboard() -> None:
    """Render the Streamlit dashboard.  Called only when Streamlit is available."""
    try:
        import streamlit as st  # type: ignore[import]
        import pandas as pd  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "The 'streamlit' and 'pandas' packages are required to run the dashboard.\n"
            "Install them with:  pip install streamlit pandas"
        ) from exc

    st.set_page_config(
        page_title="Road Safety Dashboard",
        page_icon="🚗",
        layout="wide",
    )

    st.title("🚗 Road Safety – Interactive Dashboard")
    st.markdown(
        "Explore road accident data through interactive charts. "
        "Data is loaded live from the PostgreSQL database."
    )

    # ---- Accidents by year -------------------------------------------------
    st.subheader("📅 Accidents per Year")
    try:
        year_data = fetch_accidents_by_year()
        if year_data:
            df_year = pd.DataFrame(year_data, columns=["year", "total"])
            st.bar_chart(df_year.set_index("year"))
        else:
            st.info("No data available.")
    except Exception as exc:
        st.error(f"Could not load year data: {exc}")

    col1, col2 = st.columns(2)

    # ---- Accidents by hour -------------------------------------------------
    with col1:
        st.subheader("🕐 Accidents per Hour of Day")
        try:
            hour_data = fetch_accidents_by_hour()
            if hour_data:
                df_hour = pd.DataFrame(hour_data, columns=["hour", "total"])
                st.bar_chart(df_hour.set_index("hour"))
            else:
                st.info("No data available.")
        except Exception as exc:
            st.error(f"Could not load hour data: {exc}")

    # ---- Severity distribution ---------------------------------------------
    with col2:
        st.subheader("⚠️ Severity Distribution")
        try:
            sev_data = fetch_severity_distribution()
            if sev_data:
                df_sev = pd.DataFrame(sev_data, columns=["severity", "total"])
                st.bar_chart(df_sev.set_index("severity"))
            else:
                st.info("No data available.")
        except Exception as exc:
            st.error(f"Could not load severity data: {exc}")

    # ---- Top communes ------------------------------------------------------
    st.subheader("🏙️ Top 15 Communes by Accident Count")
    try:
        commune_data = fetch_accidents_by_commune(15)
        if commune_data:
            df_commune = pd.DataFrame(commune_data, columns=["commune", "total"])
            st.bar_chart(df_commune.set_index("commune"))
        else:
            st.info("No data available.")
    except Exception as exc:
        st.error(f"Could not load commune data: {exc}")

    # ---- Weather conditions ------------------------------------------------
    st.subheader("🌦️ Accidents by Weather Condition")
    try:
        weather_data = fetch_accidents_by_weather()
        if weather_data:
            df_weather = pd.DataFrame(weather_data, columns=["condition", "total"])
            st.bar_chart(df_weather.set_index("condition"))
        else:
            st.info("No data available.")
    except Exception as exc:
        st.error(f"Could not load weather data: {exc}")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def run_dashboard() -> None:
    """Launch the Streamlit dashboard via ``road-safety dashboard``."""
    # script_path is derived from __file__ (this module's own path on disk),
    # not from any user input, so there is no injection risk here.
    script_path = __file__
    print("Launching Road Safety Dashboard…")
    print("Press Ctrl+C to stop the server.\n")
    try:
        subprocess.run(  # nosec B603 – args are fully controlled, no user input
            [sys.executable, "-m", "streamlit", "run", script_path],
            check=True,
        )
    except FileNotFoundError:
        print(
            "⚠️  'streamlit' is not installed.\nInstall it with:  pip install streamlit"
        )
    except KeyboardInterrupt:
        print("\nDashboard stopped.")


# ---------------------------------------------------------------------------
# Streamlit entry point (when invoked via `streamlit run dashboard.py`)
# ---------------------------------------------------------------------------

if __name__ == "__main__" or "streamlit" in sys.modules:  # pragma: no cover
    try:
        import streamlit as _st  # type: ignore[import]  # noqa: F401

        _render_dashboard()
    except ImportError:
        pass
