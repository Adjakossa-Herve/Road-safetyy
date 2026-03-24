from ..utils import establish_connection


def create_accident_table():
    """Creates the accidents table if it does not exist."""
    sql = """
    CREATE TABLE IF NOT EXISTS raw.accidents (
        id SERIAL PRIMARY KEY,
        type_acci VARCHAR(50),
        date_acc DATE,
        heure_acc TIME,
        commune VARCHAR(100),
        luminosite VARCHAR(100),
        cond_atmos VARCHAR(50),
        intersection VARCHAR(100),
        categorie_route VARCHAR(100),
        type_collision VARCHAR(100),
        type_vehicule_1 VARCHAR(100),
        manoeuvre_vehicule_1 VARCHAR(100),
        type_vehicule_2 VARCHAR(100),
        age_usager INT,
        sexe_usager VARCHAR(20),
        gravite_usager VARCHAR(50),
        etat_usager VARCHAR(50),
        nombre_usagers INT,
        nb_veh INT,
        nombre_pietons INT,
        nombre_motos INT,
        nombre_vl INT,
        nombre_pl INT
    );
    """
    conn = establish_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            cur.close()
            return True
        except Exception as e:
            print(f"Error creating table: {e}")
            return False
        finally:
            conn.close()
    return False
