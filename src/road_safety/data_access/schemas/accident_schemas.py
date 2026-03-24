from dataclasses import dataclass
from datetime import date, time


@dataclass
class AccidentSchema:
    type_acci: str
    date_acc: date
    heure_acc: time
    commune: str
    luminosite: str
    cond_atmos: str
    intersection: str
    categorie_route: str
    type_collision: str
    type_vehicule_1: str
    manoeuvre_vehicule_1: str
    type_vehicule_2: str
    age_usager: int
    sexe_usager: str
    gravite_usager: str
    etat_usager: str
    nombre_usagers: int
    nb_veh: int
    nombre_pietons: int
    nombre_motos: int
    nombre_vl: int
    nombre_pl: int
