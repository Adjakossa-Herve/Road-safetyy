import builtins
import os
import sys

from road_safety.runners.accident_chat import run_chat
from road_safety.runners.insights import run_insights
from road_safety.runners.map_generator import run_map, run_heatmap, run_commune_map
from road_safety.runners.dashboard import run_dashboard
from road_safety.runners.pulse import run_pulse

try:
    from road_safety.runners.accident_cli import run_menu
except Exception:  # pragma: no cover
    run_menu = None

_ORIGINAL_INPUT = builtins.input


def _choose_mode() -> str:
    forced = os.getenv("ROAD_SAFETY_MODE", "").strip().lower()
    if forced in {"menu", "free"}:
        return forced

    # If stdin isn't interactive (pytest), don't prompt unless input was monkeypatched by tests
    input_is_patched = builtins.input is not _ORIGINAL_INPUT
    if not sys.stdin.isatty() and not input_is_patched:
        return "free"

    while True:
        print("Choose a mode:")
        print("  1) Menu (options)")
        print("  2) Free commands (overview, top_communes 10, ...)")
        choice = builtins.input("> ").strip().lower()

        if choice in {"1", "menu", "m"}:
            return "menu"
        if choice in {"2", "free", "f", "commands", "cmd"}:
            return "free"

        print("Invalid choice. Please type 1 or 2.")


def main() -> int:
    args = sys.argv[1:]

    if not args:
        print("Usage: road-safety <command>")
        print("Commands: chat, insights, map, heatmap, commune-map, dashboard, pulse")
        return 1

    command = args[0].lower()

    if command == "chat":
        mode = _choose_mode()

        if mode == "menu":
            if run_menu is None:
                print("Menu mode is not available in this environment.")
                return 1
            run_menu()
            return 0

        # mode == "free"
        run_chat()
        return 0

    if command == "insights":
        run_insights()
        return 0

    if command == "map":
        output_path = args[1] if len(args) > 1 else "accidents_map.html"
        limit = int(args[2]) if len(args) > 2 else 2000
        run_map(output_path=output_path, limit=limit)
        return 0

    if command == "heatmap":
        output_path = args[1] if len(args) > 1 else "accidents_heatmap.html"
        limit = int(args[2]) if len(args) > 2 else 10000
        run_heatmap(output_path=output_path, limit=limit)
        return 0

    if command == "commune-map":
        output_path = args[1] if len(args) > 1 else "accidents_commune_map.html"
        run_commune_map(output_path=output_path)
        return 0

    if command == "dashboard":
        run_dashboard()
        return 0

    if command == "pulse":
        history_path = (
            args[1] if len(args) > 1 else "src/road_safety/data/pulse_history.jsonl"
        )
        months = int(args[2]) if len(args) > 2 else 12
        top = int(args[3]) if len(args) > 3 else 5
        run_pulse(history_path=history_path, months=months, top=top)
        return 0

    print(f"Unknown command: {command}")
    print("Usage: road-safety <command>")
    print("Commands: chat, insights, map, heatmap, commune-map, dashboard, pulse")
    return 1
