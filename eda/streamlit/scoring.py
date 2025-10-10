# coding: utf-8
# THIS SHOULD BE THE BEGINNING OF JUPYTER NOTEBOOK EXPORT
from typing import List, Dict, Set, Any

import pandas as pd
import numpy as np
import geopandas as gpd
import shapely as shp
from sklearn import preprocessing

from config import ScoringConfig

# --- Constants ---
PROJECTED_CRS = "EPSG:2154"  # RGF93 / Lambert-93, suitable for metropolitan France


# --- Data Loading Functions ---

def load_all_datasets(odis_file: str, scores_cat_file: str, metiers_file: str, formations_file: str, ecoles_file: str, maternites_file: str, sante_file: str, inclusion_file: str, sncf_file: str) -> tuple:
    """
    Loads all necessary datasets from specified file paths.
    This function acts as a facade, calling specific loading functions for each dataset.
    """
    odis = gpd.GeoDataFrame(gpd.read_parquet(odis_file))
    odis.set_geometry(odis.polygon, inplace=True)
    odis.polygon.set_precision(10**-5)
    odis = odis[~odis.polygon.isna()]
    odis.set_index('codgeo', inplace=True)

    # Index of all scores and their explanations
    scores_cat = pd.read_csv(scores_cat_file, dtype={'score': str, 'metric': str})

    #Later we need the code FAP <-> FAP Name used to classify jobs
    codfap_index = pd.read_csv(metiers_file, delimiter=';')

    # Later we need the code formation <-> Formation Name used to classify trainings
    # source: https://www.data.gouv.fr/fr/datasets/liste-publique-des-organismes-de-formation-l-6351-7-1-du-code-du-travail/
    codformations_index = pd.read_csv(formations_file, dtype={'codformation': str}).set_index('codformation')

    # Etablissements scolaires
    annuaire_ecoles = pd.read_parquet(ecoles_file)
    annuaire_ecoles.geometry = annuaire_ecoles.geometry.apply(shp.from_wkb)

    #Annuaire Maternités
    annuaire_maternites = pd.read_csv(maternites_file, delimiter=';')
    annuaire_maternites.drop_duplicates(subset=['FI_ET'], keep='last', inplace=True)

    # Annuaire etablissements santé
    annuaire_sante = pd.read_parquet(sante_file)
    annuaire_sante = annuaire_sante[annuaire_sante.LibelleSph == 'Etablissement public de santé']
    annuaire_sante['geometry'] = gpd.points_from_xy(annuaire_sante.coordxet, annuaire_sante.coordyet, crs=PROJECTED_CRS)
    annuaire_sante = pd.merge(annuaire_sante, annuaire_maternites[['FI_ET']], left_on='nofinesset', right_on='FI_ET', how='left', indicator="maternite")
    annuaire_sante.drop(columns=['FI_ET'], inplace=True)
    annuaire_sante.maternite = np.where(annuaire_sante.maternite == 'both', True, False)
    annuaire_sante['codgeo'] = annuaire_sante.Departement + annuaire_sante.Commune

    # Annuaire des services d'inclusion
    # Pre-process inclusion data for faster lookup
    annuaire_inclusion = gpd.read_parquet(inclusion_file)
    incl_index = annuaire_inclusion[['codgeo', 'categorie', 'service']].drop_duplicates()
    incl_index['key'] = incl_index.categorie+'_'+incl_index.service
    incl_index = incl_index.groupby('codgeo').agg({'key': lambda x: set(x)})

    # Carte des voies SNCF / RFF
    plan_sncf = gpd.read_file(sncf_file)
    plan_sncf = plan_sncf[plan_sncf.libelle == 'Exploitée'].to_crs(crs=PROJECTED_CRS)

    return odis, scores_cat, codfap_index, codformations_index, annuaire_ecoles, annuaire_sante, annuaire_inclusion, incl_index, plan_sncf

# --- Scoring Pipeline Functions ---

def add_distance_to_current_loc(df: gpd.GeoDataFrame, current_codgeo: str) -> gpd.GeoDataFrame:
    """
    Computes the distance from each commune in the dataframe to a reference commune.
    
    Args:
        df: GeoDataFrame with commune polygons.
        current_codgeo: The 'codgeo' of the reference commune.

    Returns:
        GeoDataFrame with an added 'dist_current_loc' column in meters.
    """
    # We first need to change CRS to a projected CRS to compute distances in meters.
    df_projected = df.to_crs(PROJECTED_CRS)

    # Isolate the reference commune and calculate its centroid.
    zone_recherche = df_projected.loc[[current_codgeo]].copy()
    zone_recherche['geometry'] = zone_recherche.centroid
    
    # Use sjoin_nearest to efficiently calculate the distance for all points.
    df_with_dist = df_projected.sjoin_nearest(zone_recherche, distance_col="dist_current_loc")[['dist_current_loc']]
    
    # Merge the distance back to the original dataframe.
    return df.merge(df_with_dist, left_index=True, right_index=True, how='left')


def filter_by_distance(df: pd.DataFrame, max_distance_km: float) -> pd.DataFrame:
    """Filters a dataframe to keep only rows within a given distance."""
    return df[df.dist_current_loc < max_distance_km * 1000].copy()

def compute_criteria_scores(df: gpd.GeoDataFrame, prefs: Dict[str, Any], incl_index: pd.DataFrame, df_all_communes: gpd.GeoDataFrame) -> gpd.GeoDataFrame: 
    """
    Computes individual scores for each criterion based on user preferences.
    All scores are normalized between 0 and 1 using a QuantileTransformer.
    """
    df = df.copy()
    
    # Use QuantileTransformer to normalize scores to a uniform distribution [0, 1].
    transformer = preprocessing.QuantileTransformer(output_distribution="uniform")

    # --- EMPLOI ---
    df['met_ratio'] = 1000 * df['met'] / df['pop_be']
    df['met_scaled'] = transformer.fit_transform(df[['met_ratio']].fillna(0))
    
    # Job categories that match user preferences
    for i in range(prefs['nb_adultes']):
        adult_key = f'adult{i+1}'
        if prefs['codes_metiers'][i]:
            prefs_metiers = set(prefs['codes_metiers'][i])
            df[f'met_match_codes_{adult_key}'] = [list(set(x).intersection(prefs_metiers)) if x is not None else [] for x in df.be_codfap_top]
            df[f'met_match_{adult_key}'] = df[f'met_match_codes_{adult_key}'].str.len()
            df[f'met_match_{adult_key}_scaled'] = transformer.fit_transform(df[[f'met_match_{adult_key}']].fillna(0))
    
    # Training centers that match
    for i in range(prefs['nb_adultes']):
        adult_key = f'adult{i+1}'
        if prefs['codes_formations'][i]:
            prefs_formations = set(prefs['codes_formations'][i])
            df[f'form_match_codes_{adult_key}'] = [list(set(x).intersection(prefs_formations)) if x is not None else [] for x in df.codes_formations]
            df[f'form_match_{adult_key}'] = df[f'form_match_codes_{adult_key}'].str.len()
            df[f'form_match_{adult_key}_scaled'] = transformer.fit_transform(df[[f'form_match_{adult_key}']].fillna(0))

    # --- HEBERGEMENT / LOGEMENT ---
    if prefs['hebergement'] == "Chez l'habitant":
        df['log_5p_ratio'] = df['rp_5+pieces'] / df['log_rp']
        df['log_5p_scaled'] = transformer.fit_transform(df[['log_5p_ratio']].fillna(0))
    
    if prefs['logement'] == "Logement Social":
        df['log_soc_inoc_ratio'] = df['log_soc_inoccupes'] / df['log_soc_total'] 
        df['log_soc_inoc_scaled'] = transformer.fit_transform(df[['log_soc_inoc_ratio']].fillna(0))
    elif prefs['logement'] == "Location":
        df['log_vac_ratio'] = df['log_vac'] / df['log_total']
        df['log_vac_scaled'] = transformer.fit_transform(df[['log_vac_ratio']].fillna(0))

    # --- EDUCATION ---
    if prefs['classe_enfants']: 
        df['risque_fermeture_ratio'] = df['risque_fermeture'] / df['ecoles_ct']
        df['classes_ferm_scaled'] = transformer.fit_transform(df[['risque_fermeture_ratio']].fillna(0))

    # --- MOBILITE ---
    # 1. Distance from the current location 
    df['reloc_dist_scaled'] = (1 - df['dist_current_loc'] / (prefs['loc_distance_km'] * 1000))
    # 2. Is the commune in the same EPCI as the current one?
    # We get the EPCI from the original, unfiltered dataframe to avoid KeyErrors
    current_epci = df_all_communes.loc[prefs['commune_actuelle']]['epci_code']
    df['reloc_epci_scaled'] = np.where(df['epci_code'] == current_epci, 1, 0)
    
    # --- SOUTIEN LOCAL ---
    if prefs['besoins_autres']:
        # Vectorized approach for 'besoins_match' - much faster than itertuples
        all_needed_services = {f"{cat}_{serv}" for cat, serv_list in prefs['besoins_autres'].items() for serv in serv_list}
        
        # Create a boolean mask for communes that have any of the needed services
        # This merges the pre-calculated incl_index with our current dataframe
        df_merged = df.join(incl_index, how='left')
        
        # Calculate the number of matching services for each commune
        df['besoins_match'] = [len(all_needed_services.intersection(s)) if isinstance(s, set) else 0 for s in df_merged['key']]
        df['besoins_match_scaled'] = transformer.fit_transform(df[['besoins_match']].fillna(0))
    else:
        # If no specific needs, score based on the general availability of inclusion services
        df['svc_incl_ratio'] = 1000 * df['svc_incl_count'] / df['pop_be']
        df['svc_incl_scaled'] = transformer.fit_transform(df[['svc_incl_ratio']].fillna(0))

    # Political orientation score
    df['pol_scaled'] = df['pol_num'].astype('float')
        
    return df


def add_neighbor_scores(df_search: gpd.GeoDataFrame, scores_cat: pd.DataFrame) -> pd.DataFrame:
    """
    Expands the dataframe to include data from neighboring communes ('voisins').
    This creates 'binome' pairs (commune + neighbor) and 'monome' cases (commune itself).
    """
    # Define columns needed for binome analysis.
    binome_columns = ['codgeo','libgeo','polygon','epci_code','epci_nom'] + scores_cat[scores_cat.incl_binome]['score'].to_list()+scores_cat[scores_cat.incl_binome]['metric'].to_list()
    binome_columns = [col for col in binome_columns if col in df_search.columns]
    df_binomes = df_search[binome_columns].copy()

    # Create a series with the commune itself and its neighbors.
    # Using .copy() on df_search prevents SettingWithCopyWarning.
    df_search_copy = df_search.copy()
    df_search_copy['codgeo_voisins_and_self'] = [
        np.append(voisins, codgeo) for voisins, codgeo in zip(df_search_copy['codgeo_voisins'], df_search_copy.index)
    ]

    # Explode the dataframe to have one row per (commune, neighbor) pair.
    df_search_exploded = df_search_copy.explode('codgeo_voisins_and_self')
    df_search_exploded.rename(columns={'codgeo_voisins_and_self':'codgeo_binome'}, inplace=True)
    
    # Merge to bring in the scores of the binome commune.
    odis_search_exploded = pd.merge(
        df_search_exploded, 
        df_binomes.add_suffix('_binome'), 
        left_on='codgeo_binome', 
        right_index=True, 
        how='inner', 
        validate="many_to_one"
    )
    
    # Add a boolean column to identify binomes (True) vs monomes (False).
    odis_search_exploded['binome'] = np.where(odis_search_exploded.index == odis_search_exploded.codgeo_binome, False, True)

    return odis_search_exploded

def compute_category_scores(df: pd.DataFrame, scores_cat: pd.DataFrame, binome_penalty: float) -> pd.DataFrame:
    """
    Aggregates individual criteria scores into category scores (e.g., 'emploi_cat_score').
    For binomes, it considers the max score between the commune and its neighbor, applying a penalty to the neighbor's score.
    """
    df = df.copy()

    for category in scores_cat['cat'].unique():
        # Get the list of score columns for the current category (e.g., ['met_scaled', 'met_match_adult1_scaled'])
        score_cols = scores_cat[scores_cat.cat == category]['score'].tolist()
        # Filter to keep only columns that actually exist in our dataframe
        score_cols = [col for col in score_cols if col in df.columns]

        if not score_cols:
            continue
        
        # For each score, calculate the effective score, which is the max of
        # (score_commune, score_voisin * (1 - penalty)).
        # This is done for all criteria in the category.
        max_scores = []
        for col in score_cols:
            score_commune = df[col]
            # Check if a corresponding binome score exists
            if f'{col}_binome' in df.columns:
                score_voisin = df[f'{col}_binome'] * (1 - binome_penalty)
                # For monomes, the binome score is NaN, so we fill it with 0 to ensure max() works correctly.
                # The score of the commune itself is not penalized.
                effective_score = np.maximum(score_commune.fillna(0), score_voisin.fillna(0))
                max_scores.append(effective_score)
            else: # This criterion is not applicable to binomes
                max_scores.append(score_commune.fillna(0))

        # The category score is the mean of the effective scores of its criteria.
        df[f'{category}_cat_score'] = pd.concat(max_scores, axis=1).mean(axis=1)
    
    return df


def compute_weighted_score(df: pd.DataFrame, config: 'ScoringConfig') -> pd.Series:
    """
    Computes the final weighted score for each row based on category scores and user-defined weights.
    """
    category_scores = [col for col in df.columns if col.endswith('_cat_score')]
    
    total_score = 0
    total_weight = 0
    
    for cat_score_col in category_scores:
        # e.g., 'emploi_cat_score' -> 'emploi'
        category_name = cat_score_col.split('_')[0]
        weight_key = f'poids_{category_name}'
        weight = getattr(config, weight_key, 0)
        
        if weight > 0:
            total_score += df[cat_score_col].fillna(0) * weight
            total_weight += weight
            
    return total_score / total_weight if total_weight > 0 else 0


def select_best_score_per_commune(df: pd.DataFrame) -> pd.DataFrame:
    """For each commune, keeps only the best scoring result (whether it's a monome or a binome)."""
    return df.sort_values('weighted_score', ascending=False).groupby('codgeo').head(1)


# --- Main Orchestration Function ---

def compute_odis_score(df_original: gpd.GeoDataFrame, scores_cat: pd.DataFrame, config: 'ScoringConfig', incl_index: pd.DataFrame) -> pd.DataFrame:
    """
    Main function that orchestrates the entire scoring pipeline.
    
    Args:
        df_original: The base GeoDataFrame of all communes, unfiltered.
        scores_cat: DataFrame defining scores and their categories.
        config: ScoringConfig object with user preferences.
        incl_index: Pre-processed DataFrame for inclusion services lookup.

    Returns:
        A DataFrame with the best score for each commune in the search area.
    """
    # Make a copy to avoid modifying the original cached dataframe
    df = df_original.copy()

    # 1. Filter communes by minimum population
    df = df[df.population > config.pop_min]

    # 2. Add distance from the user's current location
    df = add_distance_to_current_loc(df, current_codgeo=config.commune_actuelle)

    # 3. Filter by max distance to create the primary search area
    odis_search = filter_by_distance(df, max_distance_km=config.loc_distance_km)

    # 4. Compute all individual criteria scores based on preferences.
    odis_scored = compute_criteria_scores(odis_search, prefs=config.__dict__, incl_index=incl_index, df_all_communes=df_original)

    # 5. Expand the dataframe to include neighbor data (creating monomes and binomes).
    odis_exploded = add_neighbor_scores(odis_scored, scores_cat)

    # 6. Aggregate criteria scores into category scores, handling the binome logic.
    odis_exploded = compute_category_scores(odis_exploded, scores_cat=scores_cat, binome_penalty=config.binome_penalty)

    # 7. Compute the final weighted score for each commune/binome pair.
    odis_exploded['weighted_score'] = compute_weighted_score(odis_exploded, config=config)

    # 8. For each commune, keep only the best result (could be monome or a binome).
    odis_search_best = select_best_score_per_commune(odis_exploded)

    return odis_search_best
# THIS SHOULD BE THE END OF JUPYTER NOTEBOOK EXPORT
