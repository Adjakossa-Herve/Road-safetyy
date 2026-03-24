"""Interactive accident map generator.

Reads GPS coordinates from the database and produces a ``accidents_map.html``
file using *folium* (HTML/Leaflet.js).  No browser is required to generate the
file – the user can open it in any modern browser after running
``road-safety map``.

Falls back gracefully when *folium* is not installed, printing a helpful
error message instead of crashing the whole application.
"""

from __future__ import annotations

import os
from typing import Any

from ..data_access.utils import establish_connection


def _raise_helpful_runtime_if_missing_coordinates(exc: Exception) -> None:
    """Convert SQL column errors into an actionable RuntimeError for users."""
    message = str(exc).lower()
    if '"latitude"' in message and "does not exist" in message:
        raise RuntimeError(
            "Missing GPS columns in raw.accidents: latitude/longitude.\n"
            "Run: poetry run python src/road_safety/bootstrap/geocode_communes.py"
        ) from exc
    if '"longitude"' in message and "does not exist" in message:
        raise RuntimeError(
            "Missing GPS columns in raw.accidents: latitude/longitude.\n"
            "Run: poetry run python src/road_safety/bootstrap/geocode_communes.py"
        ) from exc


def fetch_coordinates(limit: int = 2000) -> list[tuple[float, float]]:
    """Return a list of (latitude, longitude) pairs from the database.

    Rows with NULL or out-of-range coordinates are excluded.
    """
    conn = establish_connection()
    if not conn:
        raise RuntimeError(
            "Database connection failed. Check DB_HOST / DB_PORT / credentials."
        )
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT latitude::float, longitude::float
                FROM raw.accidents
                WHERE latitude IS NOT NULL
                  AND longitude IS NOT NULL
                  AND latitude  BETWEEN -90  AND 90
                  AND longitude BETWEEN -180 AND 180
                LIMIT %s;
                """,
                (limit,),
            )
        except Exception as exc:
            _raise_helpful_runtime_if_missing_coordinates(exc)
            raise
        rows: list[Any] = cur.fetchall()
        cur.close()
        return [(float(lat), float(lon)) for lat, lon in rows]
    finally:
        conn.close()


def build_map(
    coordinates: list[tuple[float, float]],
    center: tuple[float, float] = (46.5, 2.5),
    zoom_start: int = 6,
) -> Any:
    """Build a *folium* Map object from a list of (lat, lon) coordinates.

    Parameters
    ----------
    coordinates:
        List of (latitude, longitude) tuples to plot as red circle markers.
    center:
        Initial map centre (defaults to metropolitan France).
    zoom_start:
        Initial zoom level.

    Returns
    -------
    A ``folium.Map`` instance.

    Raises
    ------
    ImportError
        If *folium* is not installed in the current environment.
    """
    try:
        import folium  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "The 'folium' package is required to generate maps.\n"
            "Install it with:  pip install folium"
        ) from exc

    accident_map = folium.Map(location=list(center), zoom_start=zoom_start)
    for lat, lon in coordinates:
        folium.CircleMarker(
            location=[lat, lon],
            radius=3,
            color="red",
            fill=True,
            fill_opacity=0.6,
            tooltip=f"({lat:.4f}, {lon:.4f})",
        ).add_to(accident_map)
    return accident_map


def save_map(accident_map: Any, output_path: str) -> None:
    """Save a *folium* Map to *output_path*."""
    accident_map.save(output_path)


def generate_map(output_path: str = "accidents_map.html", limit: int = 2000) -> str:
    """Fetch coordinates from the DB and write an interactive HTML map.

    Parameters
    ----------
    output_path:
        Destination file path.  Defaults to ``accidents_map.html`` in the
        current working directory.
    limit:
        Maximum number of accidents to plot (prevents huge files).

    Returns
    -------
    The resolved *output_path* string.
    """
    coords = fetch_coordinates(limit)
    accident_map = build_map(coords)
    save_map(accident_map, output_path)
    return output_path


def run_map(output_path: str = "accidents_map.html", limit: int = 2000) -> None:
    """Entry point for the ``road-safety map`` command."""
    print(f"Fetching up to {limit} accident coordinates from the database…")
    try:
        path = generate_map(output_path=output_path, limit=limit)
        print(f"✅  Interactive map saved to: {os.path.abspath(path)}")
        print("Open the file in any browser to explore the accident locations.")
    except ImportError as exc:
        print(f"⚠️  {exc}")
    except RuntimeError as exc:
        print(f"⚠️  {exc}")


# ---------------------------------------------------------------------------
# Heatmap
# ---------------------------------------------------------------------------


def build_heatmap(
    coordinates: list[tuple[float, float]],
    center: tuple[float, float] = (48.86, 2.28),
    zoom_start: int = 11,
) -> Any:
    """Build a folium HeatMap from (lat, lon) coordinates."""
    try:
        import folium
        from folium.plugins import HeatMap  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "The 'folium' package is required to generate maps.\n"
            "Install it with:  pip install folium"
        ) from exc

    m = folium.Map(location=list(center), zoom_start=zoom_start)
    HeatMap(
        [[lat, lon] for lat, lon in coordinates],
        radius=12,
        blur=10,
        max_zoom=14,
        min_opacity=0.3,
    ).add_to(m)
    return m


def generate_heatmap(
    output_path: str = "accidents_heatmap.html", limit: int = 10000
) -> str:
    coords = fetch_coordinates(limit)
    m = build_heatmap(coords)
    save_map(m, output_path)
    return output_path


def run_heatmap(
    output_path: str = "accidents_heatmap.html", limit: int = 10000
) -> None:
    """Entry point for the ``road-safety heatmap`` command."""
    print(f"Building heatmap from up to {limit} accident points…")
    try:
        path = generate_heatmap(output_path=output_path, limit=limit)
        print(f"✅  Heatmap saved to: {os.path.abspath(path)}")
        print("Open the file in any browser.")
    except ImportError as exc:
        print(f"⚠️  {exc}")
    except RuntimeError as exc:
        print(f"⚠️  {exc}")


# ---------------------------------------------------------------------------
# Commune map  (choroplèthe / bulles par commune)
# ---------------------------------------------------------------------------


def fetch_commune_stats() -> list[tuple[str, float, float, int, int, int]]:
    """Return one row per commune: (commune, lat, lon, total, fatals, risk_score)."""
    conn = establish_connection()
    if not conn:
        raise RuntimeError(
            "Database connection failed. Check DB_HOST / DB_PORT / credentials."
        )
    try:
        fatal_label = os.getenv("FATAL_LABEL", "Tue")
        severe_label = os.getenv("SEVERE_LABEL", "Blessee hospitalisee")
        light_label = os.getenv("LIGHT_LABEL", "Blessee Leger")
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT
                    commune,
                    AVG(latitude)::float   AS lat,
                    AVG(longitude)::float  AS lon,
                    COUNT(*)::int          AS total,
                    SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)::int AS fatals,
                    (3 * SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)
                     + 2 * SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)
                     + 1 * SUM(CASE WHEN gravite_usager = %s THEN 1 ELSE 0 END)
                    )::int AS risk_score
                FROM raw.accidents
                WHERE latitude IS NOT NULL
                  AND longitude IS NOT NULL
                GROUP BY commune
                ORDER BY risk_score DESC;
                """,
                (fatal_label, fatal_label, severe_label, light_label),
            )
        except Exception as exc:
            _raise_helpful_runtime_if_missing_coordinates(exc)
            raise
        rows = cur.fetchall()
        cur.close()
        return [
            (str(commune), float(lat), float(lon), int(total), int(fatals), int(rs))
            for commune, lat, lon, total, fatals, rs in rows
        ]
    finally:
        conn.close()


def build_commune_map(
    stats: list[tuple[str, float, float, int, int, int]],
    center: tuple[float, float] = (48.86, 2.28),
    zoom_start: int = 11,
) -> Any:
    """Build a bubble map: one circle per commune, sized by risk score."""
    try:
        import folium
    except ImportError as exc:
        raise ImportError(
            "The 'folium' package is required to generate maps.\n"
            "Install it with:  pip install folium"
        ) from exc

    if not stats:
        return folium.Map(location=list(center), zoom_start=zoom_start)

    max_risk = max(rs for _, _, _, _, _, rs in stats) or 1

    m = folium.Map(location=list(center), zoom_start=zoom_start)

    for commune, lat, lon, total, fatals, risk_score in stats:
        radius = 5 + 25 * (risk_score / max_risk)
        # Couleur : rouge foncé si score élevé, orange sinon
        ratio = risk_score / max_risk
        if ratio >= 0.66:
            color = "#c0392b"  # rouge
        elif ratio >= 0.33:
            color = "#e67e22"  # orange
        else:
            color = "#f1c40f"  # jaune

        folium.CircleMarker(
            location=[lat, lon],
            radius=radius,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.65,
            tooltip=(
                f"<b>{commune}</b><br>"
                f"Total accidents : {total}<br>"
                f"Décès : {fatals}<br>"
                f"Risk score : {risk_score}"
            ),
            popup=folium.Popup(
                f"<b>{commune}</b><br>Accidents : {total} | Décès : {fatals} | Score : {risk_score}",
                max_width=250,
            ),
        ).add_to(m)

    return m


def generate_commune_map(
    output_path: str = "accidents_commune_map.html",
) -> str:
    stats = fetch_commune_stats()
    m = build_commune_map(stats)
    save_map(m, output_path)
    return output_path


def run_commune_map(output_path: str = "accidents_commune_map.html") -> None:
    """Entry point for the ``road-safety commune-map`` command."""
    print("Building commune risk map (one bubble per commune)…")
    try:
        path = generate_commune_map(output_path=output_path)
        print(f"✅  Commune map saved to: {os.path.abspath(path)}")
        print("Bubble size = risk score | Red = high risk | Yellow = low risk")
        print("Open the file in any browser.")
    except ImportError as exc:
        print(f"⚠️  {exc}")
    except RuntimeError as exc:
        print(f"⚠️  {exc}")
