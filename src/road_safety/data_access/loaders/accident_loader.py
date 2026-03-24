import pandas as pd
import os
from ..utils import establish_connection
from ...config.constants import COMMUNE_CORRECTIONS, LUMINOSITY_CORRECTIONS


def clean_string_value(value):
    """Cleans a string value according to defined rules."""
    if not isinstance(value, str):
        return value
    value = value.strip()
    value = COMMUNE_CORRECTIONS.get(value, value)
    value = LUMINOSITY_CORRECTIONS.get(value, value)
    return value


def safe_convert_int(value):
    """Safely converts a value to an integer or returns None."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def load_csv_data(file_path):
    """Loads the CSV file into a DataFrame.

    Tries UTF-8 first, then falls back to common French/Windows encodings.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # ';' separator is common in French CSV exports (Excel).
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            return pd.read_csv(file_path, sep=";", encoding=enc, low_memory=False)
        except UnicodeDecodeError:
            continue

    # If all attempts failed, raise a clear error.
    raise UnicodeDecodeError(
        "utf-8", b"", 0, 1, "Unable to decode CSV with utf-8/cp1252/latin-1"
    )


def prepare_data_for_insertion(df):
    """Cleans and transforms the DataFrame for SQL insertion."""
    rows = []
    for _, row in df.iterrows():
        # Cleaning text fields
        commune = clean_string_value(row.get("commune"))
        luminosite = clean_string_value(row.get("luminosite_accident"))

        # Date/Time conversion (Expected format DD/MM/YYYY)
        try:
            date_acc = pd.to_datetime(row.get("date"), dayfirst=True).date()
        except:
            date_acc = None

        try:
            heure_acc = pd.to_datetime(row.get("heure"), format="%H:%M").time()
        except:
            heure_acc = None

        data = (
            clean_string_value(row.get("type_acci")),
            date_acc,
            heure_acc,
            commune,
            luminosite,
            clean_string_value(row.get("cond_atmos")),
            clean_string_value(row.get("Intersection")),
            clean_string_value(row.get("categorie_route")),
            clean_string_value(row.get("type_collision")),
            clean_string_value(row.get("type_vehicule_1")),
            clean_string_value(row.get("manoeuvre_vehicule_1")),
            clean_string_value(row.get("type_vehicule_2")),
            safe_convert_int(row.get("age_usager")),
            clean_string_value(row.get("sexe_usager")),
            clean_string_value(row.get("gravite_usager")),
            clean_string_value(row.get("etat_usager")),
            safe_convert_int(row.get("nombre_usagers")),
            safe_convert_int(row.get("nb_veh")),
            safe_convert_int(row.get("nombre_pietons")),
            safe_convert_int(row.get("nombre_motos")),
            safe_convert_int(row.get("nombre_vl")),
            safe_convert_int(row.get("nombre_pl")),
        )
        rows.append(data)
    return rows


def insert_accidents(data):
    """Inserts cleaned data into the PostgreSQL database."""
    sql = """
    INSERT INTO raw.accidents (
        type_acci, date_acc, heure_acc, commune, luminosite, cond_atmos,
        intersection, categorie_route, type_collision, type_vehicule_1,
        manoeuvre_vehicule_1, type_vehicule_2, age_usager, sexe_usager,
        gravite_usager, etat_usager, nombre_usagers, nb_veh, nombre_pietons,
        nombre_motos, nombre_vl, nombre_pl
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    conn = establish_connection()
    if not conn:
        return 0

    count = 0
    try:
        cur = conn.cursor()
        cur.executemany(sql, data)
        count = cur.rowcount
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Insertion error: {e}")
        conn.rollback()
    finally:
        conn.close()
    return count
