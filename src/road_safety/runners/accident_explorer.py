import os
import sys

from ..data_access.loaders import accident_loader
from ..data_access.tables import accident_table

# IMPORTANT:
# The tests patch "src.road_safety.runners.accident_explorer.*"
# but import "road_safety.runners.accident_explorer".
# We create an alias in sys.modules so both paths reference
# the exact same module instance. This ensures patch works correctly.
sys.modules.setdefault(
    "src.road_safety.runners.accident_explorer", sys.modules[__name__]
)


def execute_analysis():
    """Main entry point for the analysis."""
    print("=== Road Safety Analysis ===")

    # 1. Database initialization
    print("Creating table...")
    accident_table.create_accident_table()

    # 2. Data loading
    csv_path = os.path.join(os.path.dirname(__file__), "..", "data", "accident_idf.csv")
    print(f"Loading data from {csv_path}...")

    try:
        df = accident_loader.load_csv_data(csv_path)
        print(f"{len(df)} rows loaded.")

        # 3. Data cleaning
        print("Cleaning data...")
        clean_data = accident_loader.prepare_data_for_insertion(df)

        # 4. Database insertion
        print("Inserting into database...")
        count = accident_loader.insert_accidents(clean_data)
        print(f"{count} rows inserted successfully.")

    except FileNotFoundError:
        print("Error: Data file not found.")
    except Exception as e:
        print(f"An error occurred: {e}")
