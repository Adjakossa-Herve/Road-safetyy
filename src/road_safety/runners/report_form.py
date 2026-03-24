from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from road_safety.data_access.utils import establish_connection


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


def execute(query: str, params: tuple = ()) -> None:
    conn = establish_connection()
    if not conn:
        raise RuntimeError(
            "Database connection failed. Check DB_HOST / DB_PORT / credentials."
        )
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
        cur.close()
    finally:
        conn.close()


def list_distinct(column: str, limit: int = 30) -> list[str]:
    rows = fetch_all(
        f"""
        SELECT DISTINCT COALESCE({column}, 'UNKNOWN') AS v
        FROM raw.accidents
        WHERE {column} IS NOT NULL
        ORDER BY v
        LIMIT %s;
        """,
        (limit,),
    )
    vals = [str(r[0]) for r in rows]
    # fallback if table empty
    return vals if vals else ["UNKNOWN"]


def choose_from_list(title: str, options: list[str], allow_other: bool = True) -> str:
    print(f"\n{title}")
    for i, opt in enumerate(options, 1):
        print(f"  {i}) {opt}")
    if allow_other:
        print("  0) Autre (saisie libre)")

    while True:
        choice = input("> ").strip()
        if allow_other and choice == "0":
            v = input("Saisie libre: ").strip()
            return v if v else "UNKNOWN"
        if choice.isdigit():
            idx = int(choice)
            if 1 <= idx <= len(options):
                return options[idx - 1]
        print("Choix invalide. Recommence.")


@dataclass
class UserReport:
    full_name: str
    commune: str
    location_text: str
    vehicle_type: str
    categorie_route: str
    intersection: str
    type_collision: str
    luminosite: str
    cond_atmos: str
    gravite_usager: str
    notes: str


def insert_report(r: UserReport) -> None:
    execute(
        """
        INSERT INTO raw.user_reports
          (full_name, commune, location_text, vehicle_type, categorie_route, intersection,
           type_collision, luminosite, cond_atmos, gravite_usager, notes)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """,
        (
            r.full_name,
            r.commune,
            r.location_text,
            r.vehicle_type,
            r.categorie_route,
            r.intersection,
            r.type_collision,
            r.luminosite,
            r.cond_atmos,
            r.gravite_usager,
            r.notes,
        ),
    )


def run_report_form() -> None:
    print("\n=== Signalement d'accident (formulaire) ===")

    full_name = input("Nom et prénom: ").strip()
    if not full_name:
        print("Nom/prénom obligatoire. Annulé.")
        return

    # Dropdowns from DB
    commune = choose_from_list(
        "Choisir la commune", list_distinct("commune", 30), allow_other=True
    )
    type_collision = choose_from_list(
        "Choisir le type de collision",
        list_distinct("type_collision", 30),
        allow_other=True,
    )
    intersection = choose_from_list(
        "Choisir le type d'intersection",
        list_distinct("intersection", 30),
        allow_other=True,
    )
    categorie_route = choose_from_list(
        "Choisir la catégorie de route",
        list_distinct("categorie_route", 30),
        allow_other=True,
    )
    luminosite = choose_from_list(
        "Choisir la luminosité", list_distinct("luminosite", 30), allow_other=True
    )
    cond_atmos = choose_from_list(
        "Choisir les conditions météo",
        list_distinct("cond_atmos", 30),
        allow_other=True,
    )
    gravite = choose_from_list(
        "Choisir la gravité", list_distinct("gravite_usager", 30), allow_other=True
    )

    location_text = input("Localisation (adresse / détail): ").strip() or ""
    vehicle_type = (
        input("Véhicule (ex: VL, moto, vélo, piéton...): ").strip() or "UNKNOWN"
    )
    notes = input("Commentaire (optionnel): ").strip() or ""

    report = UserReport(
        full_name=full_name,
        commune=commune,
        location_text=location_text,
        vehicle_type=vehicle_type,
        categorie_route=categorie_route,
        intersection=intersection,
        type_collision=type_collision,
        luminosite=luminosite,
        cond_atmos=cond_atmos,
        gravite_usager=gravite,
        notes=notes,
    )

    insert_report(report)
    print("✅ Signalement enregistré.")


def list_reports(limit: int = 20) -> list[tuple[str, str, str, str]]:
    rows = fetch_all(
        """
        SELECT created_at::text, full_name, COALESCE(commune,'') AS commune, COALESCE(gravite_usager,'') AS gravite
        FROM raw.user_reports
        ORDER BY created_at DESC
        LIMIT %s;
        """,
        (limit,),
    )
    return [(str(a), str(n), str(c), str(g)) for a, n, c, g in rows]
