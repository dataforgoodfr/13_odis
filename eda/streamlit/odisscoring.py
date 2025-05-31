# coding: utf-8
# THIS SHOULD BE THE END OF JUPYTER NOTEBOOK EXPORT
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
from scipy import stats
import folium as flm #required for gdf.explore()
import shapely as shp
from shapely.wkt import loads
from shapely.geometry import Polygon
from sklearn import preprocessing
def init_loading_datasets(odis_file, scores_cat_file, metiers_file, formations_file, ecoles_file):
    odis = gpd.GeoDataFrame(gpd.read_parquet(odis_file))
    odis.set_geometry(odis.polygon, inplace=True)
    odis = odis[~odis.polygon.isna()]

    # Index of all scores and their explanations
    scores_cat = pd.read_csv(scores_cat_file)

    #Later we need the code FAP <-> FAP Name used to classify jobs
    codfap_index = pd.read_csv(metiers_file, delimiter=';')

    # Later we need the code formation <-> Formation Name used to classify trainings
    # source: https://www.data.gouv.fr/fr/datasets/liste-publique-des-organismes-de-formation-l-6351-7-1-du-code-du-travail/
    codformations_index = pd.read_csv(formations_file).set_index('codformation')

    # Etablissements scolaires
    annuaire_ecoles = pd.read_parquet(ecoles_file)
    annuaire_ecoles.geometry = annuaire_ecoles.geometry.apply(shp.from_wkb)

    return odis, scores_cat, codfap_index, codformations_index, annuaire_ecoles
# Filtering dataframe based on subject distance preference (to save on compute time later on)
def filter_loc_by_distance(df, distance):
    return df[df.dist_current_loc < distance * 1000]

# Put None as a score in the monome case
def monome_cleanup(df):
    mask = ~df['binome']
    for col in df.columns:
        if col.endswith('_binome'):
            df.loc[mask, col] = None
    return df
def adding_score_voisins(df_search, scores_cat):
    #df_search is the dataframe pre-filtered by location
    #df_source is the dataframe with all the communes
    binome_columns = ['codgeo','libgeo','polygon','epci_code','epci_nom'] + scores_cat[scores_cat.incl_binome]['score'].to_list()+scores_cat[scores_cat.incl_binome]['metric'].to_list()
    binome_columns = list(set(binome_columns) & set(df_search.columns))
    df_binomes = df_search[binome_columns].copy()

    # Adds itself to list of voisins = monome case
    # Note: this code triggers the SettingWithCopyWarning but I don't know how to fix it...
    df_search.codgeo_voisins = df_search.apply(lambda x: np.append(x.codgeo_voisins, x.codgeo), axis=1)

    # Explodes the dataframe to have a row for each voisins + itself
    df_search['codgeo_voisins_copy'] = df_search['codgeo_voisins']
    df_search_exploded = df_search.explode('codgeo_voisins_copy')
    
    # For each commune (codgeo) in search area (df_search) we add all its voisin's scores
    odis_search_exploded = pd.merge(df_search_exploded, df_binomes.add_suffix('_binome'), left_on='codgeo_voisins_copy', right_on='codgeo_binome', how='left')
    
    # Adds a column to identify binomes vs monomes + cleanup
    odis_search_exploded['binome'] = odis_search_exploded.apply(lambda x: False if x.codgeo == x.codgeo_binome else True, axis=1)
    odis_search_exploded.drop(columns={'codgeo_voisins_copy'}, inplace=True)

    #We remove all values for the monome case to avoid accounting for them in the category score calculation
    odis_search_exploded = monome_cleanup(odis_search_exploded)

    return odis_search_exploded
#Computing distance from current commune 
#Using a crs that allows to compute distance in meters for metropolitan France

def distance_calc(df, ref_point):
    return int(df.distance(ref_point))

def add_distance_to_current_loc(df, current_codgeo):
    projected_crs = "EPSG:2154"
    zone_recherche = gpd.GeoDataFrame(df[df.codgeo == current_codgeo]['polygon'])
    zone_recherche.set_geometry('polygon', inplace=True)
    zone_recherche.to_crs(projected_crs, inplace=True)

    df.to_crs(projected_crs, inplace=True)
    df['dist_current_loc'] = df['polygon'].apply(distance_calc, ref_point=zone_recherche.iloc[0].polygon)
    return df
#Adding score specific to subject looking for a job identified as en besoin
def codes_match(df, codes_list):
    #returns a list of codfaps that matches
    if df is None:
        return []
    return list(set(df.tolist()).intersection(set(codes_list)))

def fap_names_lookup(df):
    return list(codfap_index[codfap_index['Code FAP 341'].isin(df)]['Intitulé FAP 341'])
def compute_criteria_scores(df, prefs): 
    df = df.copy()
    
    # Using QuantileTransfer to normalize all scores between 0 and 1 for the region
    t = preprocessing.QuantileTransformer(output_distribution="uniform")

    #met_ration est le ratio d'offres non-pourvues pour 1000 habitants
    df['met_ratio']= 1000 * df.met/df.pop_be
    df['met_scaled'] = t.fit_transform(df[['met_ratio']].fillna(0))
    #met_tension_ratio est le ratio d'offres population de la zone (pour 1000 habitants)
    df['met_tension_ratio'] = 1000 * df.met_tension/df.pop_be
    df['met_tension_scaled'] = t.fit_transform(df[['met_tension_ratio']].fillna(0))
    #svc_ratio est le ratio de services d'inclusion de la commune (pour 1000 habitants)
    df['svc_incl_ratio'] = 1000 * df.svc_incl_count/df.pop_be
    df['svc_incl_scaled'] = t.fit_transform(df[['svc_incl_ratio']].fillna(0))
    #log_vac_ratio est le ratio de logements vacants de la commune % total logements
    df['log_vac_ratio'] = df.log_vac/df.log_total
    df['log_vac_scaled'] = t.fit_transform(df[['log_vac_ratio']].fillna(0))
    #pol est le score selon la couleur politique (extreme droite = 0, gauche = 1)
    df['pol_scaled'] = df[['pol_num']].astype('float')
    
    if prefs['hebergement'] == "Chez l'habitant":
        #log_5p+_ratio est le ratio de residences principales de 5 pièces ou plus % total residences principales
        df['log_5p_ratio'] = df['rp_5+pieces']/df.log_rp
        df['log_5p_scaled'] = t.fit_transform(df[['log_5p_ratio']].fillna(0))

    if len(prefs['classe_enfants']) > 0: 
        # Risque de fermeture école: ratio de classe à risque de fermeture % nombre d'écoles
        df['risque_fermeture_ratio'] = df.risque_fermeture/df.ecoles_ct
        df['classes_ferm_scaled'] = t.fit_transform(df[['risque_fermeture_ratio']].fillna(0))


    # Subject Specific criterias

    # We compute the distance from the current location 
    df['reloc_dist_scaled'] = (1-df['dist_current_loc']/(prefs['loc_distance_km']*1000))
    df['reloc_epci_scaled'] = np.where(df['epci_code'] == df[df.codgeo == prefs['commune_actuelle']]['epci_code'].iloc[0],1,0)

    #For each adult we look for jobs categories that match what is needed
    i=1
    for adult in prefs['codes_metiers']:
        if len(prefs['codes_metiers'][adult]) > 0:
            df['met_match_codes_adult'+str(i)] = df.be_codfap_top.apply(codes_match, codes_list=prefs['codes_metiers'][adult])
            df['met_match_adult'+str(i)] = df['met_match_codes_adult'+str(i)].apply(len)
            df['met_match_adult'+str(i)+'_scaled'] = t.fit_transform(df[['met_match_adult'+str(i)]].fillna(0))
            i+=1

    j=1
    for adult in prefs['codes_formations']:
        if len(prefs['codes_formations'][adult]) > 0:
            df['form_match_codes_adult'+str(j)] = df.codes_formations.apply(codes_match, codes_list=prefs['codes_formations'][adult])
            df['form_match_adult'+str(j)] = df['form_match_codes_adult'+str(j)].apply(len)
            df['form_match_adult'+str(j)+'_scaled'] = t.fit_transform(df[['form_match_adult'+str(j)]].fillna(0))
        j+=1

    
    return df
def compute_cat_scores(df, scores_cat, penalty):
    df = df.copy()
    df_binome = pd.DataFrame()
    columns_in_use = set(df.columns) & set(scores_cat.score)
    columns_in_use_binome = set(df.columns) & set([score+'_binome' for score in scores_cat.score])
    for cat in set(scores_cat.cat):
        cat_scores_indices = [score for score in scores_cat[scores_cat['cat'] == cat]['score'] if score in columns_in_use]
        cat_scores_indices_binome = [score+'_binome' for score in scores_cat[scores_cat['cat'] == cat]['score'] if score+'_binome' in columns_in_use_binome]

        # Efficiently select all relevant rows at once
        cat_scores_df = df[cat_scores_indices]
        for col in cat_scores_indices_binome:
            mask = df[col].notna()
            df_binome[col] = pd.to_numeric(df[col], errors='coerce')
            df_binome.loc[mask, col] = df.loc[mask, col] * (1-penalty) 
            cat_scores_df = pd.concat([cat_scores_df, df_binome[col]], axis=1)
        df[cat + '_cat_score'] = cat_scores_df.astype(float).mean(axis=1)

    return df
def compute_binome_score_old(df, binome_penalty, prefs):
    scores_col = [col for col in df.columns if col.endswith('_cat_score')]
    max_scores = pd.DataFrame()
    
    for col in scores_col:
        cat_weight = prefs[col.split('_')[0]]
        max_scores[col] = cat_weight * np.where(
            df[col] >= (1-binome_penalty)*df[col+'_binome'],
            df[col],
            (1-binome_penalty)*df[col+'_binome']
            )
    
    return max_scores.mean(axis=1).round(1)
def compute_binome_score(df, scores_cat, prefs):
    scores_cat_col = [col for col in df.columns if col.endswith('_cat_score')]
    weighted_scores = pd.DataFrame()
    for col in scores_cat_col:
        cat_weight =  prefs['poids_'+col.split('_')[0]]
        weighted_scores[col] = cat_weight * df[col]
    
    return weighted_scores.astype(float).mean(axis=1)
def best_score_compute(df):
    #Keeping the best (top #1) monome or binome result for each commune
    best = df.sort_values('weighted_score', ascending=False).groupby('codgeo').head(1)
    return best
#Main function that aggregates most of the above in one sequence
def compute_odis_score(df, scores_cat, prefs):
    df = add_distance_to_current_loc(df, current_codgeo=prefs['commune_actuelle'])

    # We filter by distance to reduce the compute cost on a smaller odis_search dataframe
    odis_search = filter_loc_by_distance(df, distance=prefs['loc_distance_km'])

    # We compute the subject specific scores
    odis_scored = compute_criteria_scores(odis_search, prefs=prefs)

    # We add the criteria scores for all neighbor communes forming monomes and binomes
    odis_exploded = adding_score_voisins(odis_scored, scores_cat)

    # We compute the category scores for both the target and the binome
    odis_exploded = compute_cat_scores(odis_exploded, scores_cat=scores_cat, penalty=prefs['binome_penalty'])

    # We computing the final weighted score for all commune<->voisin combinations
    odis_exploded['weighted_score'] = compute_binome_score(odis_exploded, scores_cat=scores_cat, prefs=prefs)

    # We keep best monome or binome for each commune 
    odis_search_best = best_score_compute(odis_exploded)

    return odis_search_best
# THIS SHOULD BE THE END OF JUPYTER NOTEBOOK EXPORT
