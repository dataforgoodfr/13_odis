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
def init_loading_datasets(odis_file, scores_cat_file, metiers_file, formations_file, ecoles_file, maternites_file, sante_file, inclusion_file):
    odis = gpd.GeoDataFrame(gpd.read_parquet(odis_file))
    odis.set_geometry(odis.polygon, inplace=True)
    odis.polygon.set_precision(10**-5)
    odis = odis[~odis.polygon.isna()]
    odis.set_index('codgeo', inplace=True)

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

    #Annuaire Maternités
    # Source: https://www.data.gouv.fr/fr/datasets/liste-des-maternites-de-france-depuis-2000/
    annuaire_maternites = pd.read_csv(maternites_file, delimiter=';')
    annuaire_maternites.drop_duplicates(subset=['FI_ET'], keep='last', inplace=True)
    annuaire_maternites.head()

    # Annuaire etablissements santé
    # Source: https://www.data.gouv.fr/fr/datasets/reexposition-des-donnees-finess/
    annuaire_sante = pd.read_parquet(sante_file)
    annuaire_sante = annuaire_sante[annuaire_sante.LibelleSph == 'Etablissement public de santé']
    annuaire_sante['geometry'] = gpd.points_from_xy(annuaire_sante.coordxet, annuaire_sante.coordyet, crs='epsg:2154')
    annuaire_sante = pd.merge(annuaire_sante, annuaire_maternites[['FI_ET']], left_on='nofinesset', right_on='FI_ET', how='left', indicator="maternite")
    annuaire_sante.drop(columns=['FI_ET'], inplace=True)
    annuaire_sante.maternite = np.where(annuaire_sante.maternite == 'both', True, False)
    annuaire_sante['codgeo'] = annuaire_sante.Departement + annuaire_sante.Commune

    # Annuaire des services d'inclusion
    annuaire_inclusion = gpd.read_parquet(inclusion_file)
    incl_index=annuaire_inclusion[['codgeo', 'categorie', 'service']].drop_duplicates()
    incl_index['key'] = incl_index.categorie+'_'+incl_index.service
    incl_index=incl_index.groupby('codgeo').agg({'key':lambda x: set(x)})

    return odis, scores_cat, codfap_index, codformations_index, annuaire_ecoles, annuaire_sante, annuaire_inclusion, incl_index
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

    binome_columns = ['codgeo','libgeo','polygon','epci_code','epci_nom'] + scores_cat[scores_cat.incl_binome]['score'].to_list()+scores_cat[scores_cat.incl_binome]['metric'].to_list()
    # We take the subset of possible score columns that actually exist in our dataframe
    binome_columns = list(set(binome_columns) & set(df_search.columns))
    df_binomes = df_search[binome_columns].copy()

    # Adds itself to list of voisins = monome case
    # Note: this code triggers the SettingWithCopyWarning but I don't know how to fix it...
    df_search.codgeo_voisins = df_search.apply(lambda x: np.append(x.codgeo_voisins, x.name), axis=1)

    # Explodes the dataframe to have a row for each voisins + itself
    df_search['codgeo_voisins_copy'] = df_search['codgeo_voisins']
    df_search_exploded = df_search.explode('codgeo_voisins_copy')
    df_search_exploded.rename(columns={'codgeo_voisins_copy':'codgeo_binome'}, inplace=True)
    
    # For each commune (codgeo) in search area (df_search) we add all its voisin's scores
    odis_search_exploded = pd.merge(
        df_search_exploded, 
        df_binomes.add_suffix('_binome'), 
        left_on='codgeo_binome', 
        right_index=True, 
        how='inner', 
        validate="many_to_one")
    
    # Adds a column to identify binomes vs monomes + cleanup
    odis_search_exploded['binome'] = np.where(odis_search_exploded.index == odis_search_exploded.codgeo_binome, True, False)

 
    #We remove all values for the monome case to avoid accounting for them in the category score calculation
    odis_search_exploded = monome_cleanup(odis_search_exploded)

    return odis_search_exploded
#Computing distance from current commune 
#Using a crs that allows to compute distance in meters for metropolitan France

def distance_calc(df, ref_point):
    return int(df.distance(ref_point))

def add_distance_to_current_loc(df, current_codgeo):
    projected_crs = "EPSG:2154"
    # We first need to change CRS to a projected CRS
    df_projected = gpd.GeoDataFrame(df)
    df_projected = df_projected.to_crs(projected_crs)

    zone_recherche = df_projected[df_projected.index == current_codgeo].copy()
    zone_recherche['centroid'] = zone_recherche.centroid
    zone_recherche = gpd.GeoDataFrame(zone_recherche, geometry='centroid')
    zone_recherche.to_crs(projected_crs, inplace=True)
    
    df_projected = df_projected.sjoin_nearest(zone_recherche, distance_col="dist_current_loc")[['dist_current_loc']]
    df = pd.merge(df, df_projected, left_index=True, right_index=True, how='left')
    
    return df
#Adding score specific to subject looking for a job identified as en besoin
def codes_match(df, codes_list):
    #returns a list of codfaps that matches
    if df is None:
        return []
    return list(set(df.tolist()).intersection(set(codes_list)))

def fap_names_lookup(df):
    return list(codfap_index[codfap_index['Code FAP 341'].isin(df)]['Intitulé FAP 341'])
def compute_criteria_scores(df, prefs, incl_index): 
    df = df.copy()
    
    # Using QuantileTransfer to normalize all scores between 0 and 1 for the region
    t = preprocessing.QuantileTransformer(output_distribution="uniform")

    # EMPLOI

    #met_ration est le ratio d'offres non-pourvues pour 1000 habitants
    df['met_ratio']= 1000 * df['met']/df['pop_be']
    df['met_scaled'] = t.fit_transform(df[['met_ratio']].fillna(0))
    
    #met_tension_ratio est le ratio d'offres pour des métiers déclarés en tensions sur la zone (pour 1000 habitants)
    # df['met_tension_ratio'] = 1000 * df['met_tension']/df['pop_be']
    # df['met_tension_scaled'] = t.fit_transform(df[['met_tension_ratio']].fillna(0))

    # jobs categories that match
    for adult in range(0,prefs['nb_adultes']):
        if len(prefs['codes_metiers'][adult]) > 0:
            df['met_match_codes_adult'+str(adult+1)] = df.be_codfap_top.apply(codes_match, codes_list=prefs['codes_metiers'][adult])
            df['met_match_adult'+str(adult+1)] = df['met_match_codes_adult'+str(adult+1)].apply(len)
            df['met_match_adult'+str(adult+1)+'_scaled'] = t.fit_transform(df[['met_match_adult'+str(adult+1)]].fillna(0))
    
    # training centers that match
    for adult in range(0,prefs['nb_adultes']):
        if len(prefs['codes_formations'][adult]) > 0:
            df['form_match_codes_adult'+str(adult+1)] = df.codes_formations.apply(codes_match, codes_list=prefs['codes_formations'][adult])
            df['form_match_adult'+str(adult+1)] = df['form_match_codes_adult'+str(adult+1)].apply(len)
            df['form_match_adult'+str(adult+1)+'_scaled'] = t.fit_transform(df[['form_match_adult'+str(adult+1)]].fillna(0))

    
    # HEBERGEMENT / LOGEMENT
    if prefs['hebergement'] == "Chez l'habitant":
        #log_5p+_ratio est le ratio de residences principales de 5 pièces ou plus % total residences principales
        df['log_5p_ratio'] = df['rp_5+pieces']/df['log_rp']
        df['log_5p_scaled'] = t.fit_transform(df[['log_5p_ratio']].fillna(0))
    
    if prefs['logement'] == "Logement Social":
        # log_soc_inoccupes = nombre de logements sociaux vacants + vides
        df['log_soc_inoc_ratio'] = df['log_soc_inoccupes']/df['log_soc_total'] 
        df['log_soc_inoc_scaled'] = t.fit_transform(df[['log_soc_inoc_ratio']].fillna(0))
    elif prefs['logement'] == "Location":
        #log_vac_ratio est le ratio de logements vacants de la commune % total logements
        df['log_vac_ratio'] = df['log_vac']/df['log_total']
        df['log_vac_scaled'] = t.fit_transform(df[['log_vac_ratio']].fillna(0))

    
    # EDUCATION
    if len(prefs['classe_enfants']) > 0: 
        # Risque de fermeture école: ratio de classe à risque de fermeture % nombre d'écoles
        df['risque_fermeture_ratio'] = df['risque_fermeture']/df['ecoles_ct']
        df['classes_ferm_scaled'] = t.fit_transform(df[['risque_fermeture_ratio']].fillna(0))

    # MOBILITE

    # 1. distance from the current location 
    df['reloc_dist_scaled'] = (1-df['dist_current_loc']/(prefs['loc_distance_km']*1000))
    df['reloc_epci_scaled'] = np.where(df['epci_code'] == df.loc[prefs['commune_actuelle']]['epci_code'],1,0)

    
    # SOUTIEN LOCAL
    # other needs facilities
    if bool(prefs['besoins_autres']):
        # We keep things pretty simple here: for every need type, if there is at least corresponding facility in the same geo or nearby geo we score 1
        df['besoins_match'] = 0
        for row in df.itertuples():
            match_counter = 0
            # codgeos = row.codgeo_voisins.tolist()+[row.Index] if row.codgeo_voisins is not None else [row.Index]
            codgeos = [row.Index] # For now we simplify and only look at the specific geos and not neighboring communes
            for codgeo in codgeos:
                if codgeo in incl_index.index:
                    for key, values in prefs['besoins_autres'].items():
                        for v in values:
                            # if v is None:
                            #     if key in incl_index.loc[codgeo].item():
                            #         match_counter += 1
                            # else: 
                            if key+'_'+v in incl_index.loc[codgeo].item():
                                match_counter += 1
            df.loc[row.Index, 'besoins_match'] = match_counter
        df['besoins_match_scaled'] = t.fit_transform(df[['besoins_match']].fillna(0))
    else:
        #svc_ratio est le ratio de services d'inclusion de la commune (pour 1000 habitants)
        df['svc_incl_ratio'] = 1000 * df['svc_incl_count']/df['pop_be']
        df['svc_incl_scaled'] = t.fit_transform(df[['svc_incl_ratio']].fillna(0))

    #pol est le score selon la couleur politique (extreme droite = 0, gauche = 1)
    df['pol_scaled'] = df[['pol_num']].astype('float')
        
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
# def compute_binome_score(df, scores_cat, prefs):
#     scores_cat_col = [col for col in df.columns if col.endswith('_cat_score')]
#     weighted_scores = pd.DataFrame()
#     for cat_score in scores_cat_col:
#         cat_weight =  prefs['poids_'+cat_score.split('_')[0]]
#         weighted_scores[cat_score] = cat_weight * df[cat_score]
    
#     return weighted_scores.astype(float).mean(axis=1)

def compute_binome_score(df, scores_cat, prefs):
    scores_cat_col = [col for col in df.columns if col.endswith('_cat_score')]
    weighted_scores = 0
    total_weight = 0
    for cat_score in scores_cat_col:
        cat_weight =  prefs['poids_'+cat_score.split('_')[0]]
        weighted_scores += cat_weight * df[cat_score]
        total_weight += cat_weight
    
    return weighted_scores / total_weight
def best_score_compute(df):
    #Keeping the best (top #1) monome or binome result for each commune
    best = df.sort_values('weighted_score', ascending=False).groupby('codgeo').head(1)
    return best
#Main function that aggregates most of the above in one sequence
def compute_odis_score(df, scores_cat, prefs, incl_index):
    
    # We restrict to communes with pop larger than 1000 (30% of all communes)
    df = df[df.population > 1000]

    # We add the disctance from the current location defined in the app
    df = add_distance_to_current_loc(df, current_codgeo=prefs['commune_actuelle'])

    # We filter by distance to reduce the compute cost on a smaller odis_search dataframe
    odis_search = filter_loc_by_distance(df, distance=prefs['loc_distance_km'])

    # We compute the subject specific scores
    odis_scored = compute_criteria_scores(odis_search, prefs=prefs, incl_index=incl_index)

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
