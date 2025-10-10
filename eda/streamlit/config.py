# /home/jacques/odis/13_odis/eda/streamlit/config.py
from dataclasses import dataclass, field
from typing import List, Dict, Any

# --- File Paths ---
ODIS_FILE = '../csv/odis_june_2025_jacques.parquet'
SCORES_CAT_FILE = '../csv/odis_scores_cat.csv'
METIERS_FILE = '../csv/dares_nomenclature_fap2021.csv'
FORMATIONS_FILE = '../csv/index_formations.csv'
ECOLES_FILE = '../csv/annuaire_ecoles_france_mini.parquet'
MATERNITE_FILE = '../csv/annuaire_maternites_DREES.csv'
SANTE_FILE = '../csv/annuaire_sante_finess.parquet'
INCLUSION_FILE = '../csv/odis_services_incl_exploded.parquet'
SNCF_FILE = '../csv/formes-des-lignes-du-rfn.geojson'

# --- Map Defaults ---
DEFAULT_MAP_CENTER = [46.603354, 1.888334] # Center of France

# --- Scoring Configuration ---
@dataclass
class ScoringConfig:
    """
    A dataclass to hold all user preferences and scoring parameters.
    This provides type safety and autocompletion in IDEs.
    """
    # Weights
    poids_emploi: int
    poids_logement: int
    poids_education: int
    poids_inclusion: int
    poids_mobilité: int
    
    # Location
    commune_actuelle: str
    loc_distance_km: int
    
    # Household
    nb_adultes: int
    nb_enfants: int
    
    # Preferences
    hebergement: str
    logement: str
    codes_metiers: List[List[str]]
    codes_formations: List[List[str]]
    classe_enfants: List[str]
    besoin_sante: str
    besoins_autres: Dict[str, List[str]]
    
    # Technical parameters
    binome_penalty: float
    pop_min: int

# --- Demo Scenarios ---
DEMO_DATA_DEFAULT = {
    'nom': None,
    'poids_emploi': 100,
    'poids_logement': 100,
    'poids_education': 100,
    'poids_inclusion': 25,
    'poids_mobilité': 100,
    'departement_actuel': '33',
    'commune_actuelle': 'Bordeaux',
    'loc_distance_km': 50,
    'hebergement': 'Location',
    'logement': 'Location',
    'sante': "Aucun",
    'nb_adultes': 1,
    'nb_enfants': 0,
    'codes_metiers': [],
    'codes_formations': [],
    'classe_enfants': [],
    'binome_penalty': 0.5,
    'pop_min': 1000,
    'besoins_autres': {}
}

DEMO_SCENARIOS = {
    "1": {
        'nom': 'Zacharie',
        'departement_actuel': '33',
        'commune_actuelle': 'Bordeaux',
        'loc_distance_km': 50,
        'hebergement': "Chez l'habitant",
        'nb_adultes': 1,
        'nb_enfants': 0,
        'poids_mobilité': 50,
    },
    "2": {
        'nom': 'Olga & Dimitri',
        'departement_actuel': '75',
        'commune_actuelle': 'Paris',
        'loc_distance_km': 1000,
        'hebergement': "Location",
        'logement': "Logement Social",
        'nb_adultes': 2,
        'nb_enfants': 2,
        'codes_metiers': [['B2X37', 'B2X38'], []],
        'codes_formations': [[], ['331', '330', '326']],
        'classe_enfants': ['Maternelle', 'Elémentaire'],
        'sante': "Maternité",
        'poids_mobilité': 0,
    },
    "3": {
        'nom': 'Aïcha',
        'departement_actuel': '13',
        'commune_actuelle': 'Marseille',
        'loc_distance_km': 50,
        'hebergement': "Location",
        'logement': "Logement Social",
        'nb_adultes': 1,
        'nb_enfants': 2,
        'codes_metiers': [['T2A60']],
        'classe_enfants': ['Elémentaire', 'Collège'],
        'besoins_autres': {'apprendre-francais': ['-']},
        'poids_mobilité': 50,
        'poids_inclusion': 50,
        'poids_emploi': 100,
    }
}
