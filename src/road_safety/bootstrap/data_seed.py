from __future__ import annotations

import os
from pathlib import Path

from road_safety.data_access.utils import establish_connection
from road_safety.bootstrap import load_dotenv_if_present
from road_safety.data_access.loaders.accident_loader import (
    load_csv_data,
    prepare_data_for_insertion,
    insert_accidents,
)


def _is_pytest_running() -> bool:
    # pytest sets this during test execution
    return os.getenv("PYTEST_CURRENT_TEST") is not None


def ensure_accidents_loaded() -> None:
    """
    If RS_AUTO_LOAD=1 and table raw.accidents is empty, load CSV into DB.
    Safe:
      - does nothing during pytest
      - does nothing if DB is unreachable
      - does nothing if table already has data
    """
    if _is_pytest_running():
        return

    # Ensure .env and defaults are loaded before reading RS_AUTO_LOAD.
    load_dotenv_if_present()

    if os.getenv("RS_AUTO_LOAD", "0") != "1":
        return

    conn = establish_connection()
    if not conn:
        # DB not running, do not crash the app
        return

    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM raw.accidents;")
        row = cur.fetchone()
        count = int(row[0]) if row is not None else 0
        cur.close()
    finally:
        conn.close()

    if count > 0:
        return

    # Resolve CSV path robustly (project root)
    project_root = (
        Path(__file__).resolve().parents[3]
    )  # .../src/road_safety/bootstrap -> root
    csv_path = project_root / "src" / "road_safety" / "data" / "accident_idf.csv"

    raw = load_csv_data(str(csv_path))
    clean = prepare_data_for_insertion(raw)
    insert_accidents(clean)
