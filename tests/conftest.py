import pytest
import pandas as pd


@pytest.fixture
def sample_dataframe():
    """Creates a mock DataFrame for testing."""
    data = {
        "type_acci": ["Leger", "Grave"],
        "date": ["19/02/2010", "23/06/2009"],
        "heure": ["16:45", "13:00"],
        "commune": ["Rueil-Malmaison", "Non renseignee nterre"],
        "luminosite_accident": ["Plein jour", "Nuit sanseclairage public"],
        "cond_atmos": ["Autre", "Autre"],
        "Intersection": ["Hors intersection", "Intersection en T"],
        "categorie_route": ["Autoroute", "Route departementale"],
        "type_collision": ["Collisions en chaine", "Collision par l'arriere"],
        "type_vehicule_1": ["VL seul", "Motocyclette > 125 cm3"],
        "manoeuvre_vehicule_1": ["Meme sens | Meme file", "Meme sens | Meme file"],
        "type_vehicule_2": ["VL seul", "VL seul"],
        "age_usager": [47, 26],
        "sexe_usager": ["Feminin", "Masculin"],
        "gravite_usager": ["Blessee Leger", "Indemne"],
        "etat_usager": ["Non renseignee ", "Non renseignee "],
        "nombre_usagers": [3, 3],
        "nb_veh": [3, 2],
        "nombre_pietons": [0, 0],
        "nombre_motos": [0, 1],
        "nombre_vl": [3, 1],
        "nombre_pl": [0, 0],
    }
    return pd.DataFrame(data)
