from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

from . import accident_db as db


HELP_TEXT = """\
Road Safety interactive CLI (menu)

0) Quit
1) Overview (severity breakdown)
2) Fatal rate
3) Collisions (top)
4) Top communes
5) Statistiques urgences (raw.accidents)
6) Extended Analytics + Dashboard
"""


@dataclass(frozen=True)
class MenuItem:
    key: str
    label: str
    action: Callable[[], None]


def _ask_int(prompt: str, default: Optional[int] = None) -> int:
    while True:
        raw = input(prompt).strip()
        if raw == "" and default is not None:
            return default
        try:
            return int(raw)
        except ValueError:
            print("Please enter an integer.")


def action_overview() -> None:
    rows = db.compute_severity_breakdown()
    db.print_table(["gravite_usager", "total"], rows)


def action_fatal_rate() -> None:
    rate, fatalities, total = db.compute_fatal_rate()
    db.print_table(["fatal_rate_%", "fatalities", "total"], [(rate, fatalities, total)])


def action_collisions() -> None:
    rows = db.list_collision_types()
    db.print_table(["type_collision", "total"], rows[:20])


def action_top_communes() -> None:
    n = _ask_int("How many communes (default 10)? ", default=10)
    rows = db.list_top_communes(n)
    db.print_table(["commune", "total"], rows)


def action_columns_raw_accidents() -> None:
    rows = db.fetch_table_columns("raw", "accidents")
    db.print_table(["column_name", "data_type"], rows)


def action_operational_stats() -> None:
    from .accident_chat import fetch_operational_stats

    fetch_operational_stats()


# ---------------------------------------------------------------------------
# Extended Analytics menu + Decision Dashboard
# ---------------------------------------------------------------------------


def _show_dashboard(results: list[tuple[str, list[str], list[Any]]]) -> None:
    W = 54
    print(f"\n\n{'=' * W}")
    print(f"{'  EMERGENCY DECISION DASHBOARD':^{W}}")
    print(f"{'=' * W}")

    priority: dict[str, int] = {}

    for title, headers, rows in results:
        print(f"\n{title}")
        print("-" * len(title))
        db.print_table(headers, rows)
        # Accumulate scores for priority ranking from first column
        for row in rows:
            commune = str(row[0])
            score = int(row[-1]) if len(row) > 1 else 1
            priority[commune] = priority.get(commune, 0) + score

    if priority:
        top5 = sorted(priority.items(), key=lambda x: -x[1])[:5]
        print(f"\n{'=' * W}")
        print("  Zones prioritaires suggérées :")
        for i, (commune, _) in enumerate(top5, 1):
            print(f"    {i}) {commune}")
    print(f"{'=' * W}")


def run_extended_menu() -> None:
    """Menu interactif Extended Analytics avec tableau de bord final."""
    results: list[tuple[str, list[str], list[Any]]] = []

    while True:
        print("\n=== Extended Analytics ===")
        print("  1) Top communes les plus meurtrières")
        print("  2) Top communes accidents graves")
        print("  3) Classement risk score (communes)")
        print("  4) Risk score d'une commune")
        print("  5) Tendance accidentologie (plage de dates)")
        print("  6) Tendance pour une commune")
        print("  0) Retour + afficher le tableau de bord")

        choice = input("\n> ").strip()

        if choice == "0":
            _show_dashboard(results)
            return

        elif choice == "1":
            n = _ask_int("Combien de communes (défaut 10)? ", default=10)
            rows = db.list_top_fatal_communes(n)
            headers = ["commune", "décès"]
            db.print_table(headers, rows)
            results.append((f"Top {n} communes meurtrières", headers, rows))

        elif choice == "2":
            n = _ask_int("Combien de communes (défaut 10)? ", default=10)
            rows = db.list_top_severe_communes(n)
            headers = ["commune", "accidents_graves"]
            db.print_table(headers, rows)
            results.append((f"Top {n} communes accidents graves", headers, rows))

        elif choice == "3":
            n = _ask_int("Combien de communes (défaut 10)? ", default=10)
            rows = db.compute_risk_score_by_commune(n)
            headers = ["commune", "décès", "grave", "léger", "risk_score"]
            db.print_table(headers, rows)
            results.append((f"Risk Score Top {n}", headers, rows))

        elif choice == "4":
            commune = input("Nom de la commune : ").strip()
            f, s, l, rs = db.compute_commune_risk_score(commune)
            rows = [(commune, f, s, l, rs)]
            headers = ["commune", "décès", "grave", "léger", "risk_score"]
            db.print_table(headers, rows)
            results.append((f"Risk Score — {commune}", headers, rows))

        elif choice == "5":
            start = input("Date début (YYYY-MM-DD) : ").strip()
            end = input("Date fin   (YYYY-MM-DD) : ").strip()
            rows = db.compute_trend_days(start, end)
            headers = ["jour", "total"]
            db.print_table(headers, rows)
            results.append((f"Tendance {start} → {end}", headers, rows))

        elif choice == "6":
            start = input("Date début (YYYY-MM-DD) : ").strip()
            end = input("Date fin   (YYYY-MM-DD) : ").strip()
            commune = input("Nom de la commune : ").strip()
            rows = db.compute_trend_days(start, end, commune)
            headers = ["jour", "total"]
            db.print_table(headers, rows)
            results.append((f"Tendance {commune} ({start}→{end})", headers, rows))

        else:
            print("Option invalide.")


def action_extended_menu() -> None:
    run_extended_menu()


def run_menu() -> None:
    print("=== Road Safety Interactive (Menu) ===")
    print("Type 'help' to show commands. Choose an option number.\n")

    items = [
        MenuItem("1", "Overview (severity breakdown)", action_overview),
        MenuItem("2", "Fatal rate", action_fatal_rate),
        MenuItem("3", "Collisions (top)", action_collisions),
        MenuItem("4", "Top communes", action_top_communes),
        MenuItem(
            "5", "Statistiques urgences (raw.accidents)", action_operational_stats
        ),
        MenuItem("6", "Extended Analytics + Dashboard", action_extended_menu),
    ]
    by_key = {it.key: it for it in items}

    while True:
        print("Menu:")
        print("  0) Quit")
        for it in items:
            print(f"  {it.key}) {it.label}")
        choice = input("\n> ").strip().lower()

        if choice in {"0", "q", "quit", "exit"}:
            print("Bye.")
            return
        if choice in {"help", "h", "?"}:
            print(HELP_TEXT)
            continue

        item = by_key.get(choice)
        if not item:
            print("Unknown option. Type 'help' or choose a number.")
            continue

        try:
            item.action()
        except Exception as e:
            print(f"Error: {e}")
