"""Géocode les communes de raw.accidents via l'API Nominatim (OpenStreetMap)
et met à jour les colonnes latitude / longitude de la table.

Usage
-----
    poetry run python src/road_safety/bootstrap/geocode_communes.py
    # ou via le Makefile :
    make geocode

Stratégie
---------
- Récupère les communes DISTINCTES sans coordonnées (une seule requête réseau
  par commune, même si elle contient des milliers d'accidents).
- Respecte la limite Nominatim : 1 requête/seconde avec User-Agent identifiable.
- Met à jour la base par commune (UPDATE SET ... WHERE LOWER(commune) = ...).
- Idempotent : peut être relancé sans risque.
"""
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from typing import Optional

from road_safety.data_access.utils import establish_connection

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "road-safety-analytics/1.0 (educational-project)"
RATE_LIMIT_SEC = 1.1  # Nominatim ToS : max 1 req/s
REGION = "Île-de-France"


def geocode_commune(commune: str) -> Optional[tuple[float, float]]:
    """Retourne (latitude, longitude) pour une commune en Île-de-France."""
    query = f"{commune}, {REGION}, France"
    params = urllib.parse.urlencode({
        "format": "json",
        "q": query,
        "limit": 1,
        "countrycodes": "fr",
    })
    req = urllib.request.Request(
        f"{NOMINATIM_URL}?{params}",
        headers={"User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as exc:
        print(f"  ⚠  Erreur géocodage {commune!r}: {exc}")
    return None


def run_geocoding() -> None:
    conn = establish_connection()
    if not conn:
        print("Erreur : impossible de se connecter à la base de données.")
        return

    try:
        cur = conn.cursor()

        # 1. Ajouter les colonnes si absentes
        cur.execute("""
            ALTER TABLE raw.accidents
                ADD COLUMN IF NOT EXISTS latitude  FLOAT,
                ADD COLUMN IF NOT EXISTS longitude FLOAT;
        """)
        conn.commit()
        print("✓ Colonnes latitude / longitude vérifiées.")

        # 2. Communes sans coordonnées
        cur.execute("""
            SELECT DISTINCT commune
            FROM raw.accidents
            WHERE commune IS NOT NULL
              AND (latitude IS NULL OR longitude IS NULL)
            ORDER BY commune;
        """)
        communes = [row[0] for row in cur.fetchall()]

        if not communes:
            print("✓ Toutes les communes sont déjà géocodées.")
            return

        total = len(communes)
        print(f"\n{total} communes à géocoder (≈ {total * RATE_LIMIT_SEC:.0f}s)…\n")

        ok = ko = 0
        for i, commune in enumerate(communes, 1):
            print(f"  [{i:>3}/{total}] {commune:<30}", end=" ", flush=True)
            coords = geocode_commune(commune)

            if coords:
                lat, lon = coords
                cur.execute(
                    """
                    UPDATE raw.accidents
                    SET latitude  = %s,
                        longitude = %s
                    WHERE LOWER(commune) = LOWER(%s);
                    """,
                    (lat, lon, commune),
                )
                conn.commit()
                print(f"✓  ({lat:.4f}, {lon:.4f})")
                ok += 1
            else:
                print("✗  non trouvée")
                ko += 1

            time.sleep(RATE_LIMIT_SEC)

        cur.close()
        print(f"\n{'=' * 50}")
        print(f"  Géocodage terminé : {ok} OK — {ko} échecs")
        print(f"  Lancez maintenant : make map")
        print(f"{'=' * 50}")

    finally:
        conn.close()


if __name__ == "__main__":
    run_geocoding()
