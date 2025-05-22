# coding: utf-8

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
df = gpd.read_parquet('../csv/odis_april_2025_jacques.parquet')
odis = gpd.GeoDataFrame(df)
odis.set_geometry(odis.polygon, inplace=True)
odis = odis[~odis.polygon.isna()]
# As we work and to save on compute we filter for a single region
#odis=odis[odis.reg_code == '75'].copy()
#Later we need the code FAP <-> FAP Name used to classify jobs
codfap_index = pd.read_csv('../csv/dares_nomenclature_fap2021.csv', delimiter=';')

# Later we need the code formation <-> Formation Name used to classify trainings
# source: https://www.data.gouv.fr/fr/datasets/liste-publique-des-organismes-de-formation-l-6351-7-1-du-code-du-travail/
codformations_index = pd.read_csv('../csv/index_formations.csv').set_index('codformation')

# Etablissements scolaires
annuaire_ecoles = pd.read_parquet('../csv/annuaire_ecoles_france_mini.parquet')
annuaire_ecoles.geometry = annuaire_ecoles.geometry.apply(shp.from_wkb)
#met_ratio est le ratio d'offres sur la population de la zone (pour 1000 habitants)
odis['met_ratio']= 1000 * odis.met/odis.pop_be
#met_tension_ratio est le ratio d'offres population de la zone (pour 1000 habitants)
#odis['met_tension_ratio'] = np.where(odis.met_tension == 0, None, 1000 * odis.met_tension/odis.pop_be)
odis['met_tension_ratio'] = 1000 * odis.met_tension/odis.pop_be

#svc_ratio est le ratio de services d'inclusion de la commune (pour 1000 habitants)
#odis['svc_incl_ratio'] = np.where(odis.svc_incl_count == 0, None, 1000 * odis.svc_incl_count/odis.pop_be)
odis['svc_incl_ratio'] = 1000 * odis.svc_incl_count/odis.pop_be

#log_vac_ratio est le ratio de logements vacants de la commune % total logements
#odis['log_vac_ratio'] = np.where(odis.log_vac == 0, None, 1000 * odis.log_vac/odis.log_total)
odis['log_vac_ratio'] = odis.log_vac/odis.log_total

#log_5p+_ratio est le ratio de residences principales de 5 pièces ou plus % total residences principales
#odis['log_5p_ratio'] = np.where(odis['rp_5+pieces'] == 0, None, 1000 * odis['rp_5+pieces']/odis.log_rp)
odis['log_5p_ratio'] = odis['rp_5+pieces']/odis.log_rp

# Risque de fermeture école: ratio de classe à risque de fermeture % nombre d'écoles
odis['risque_fermeture_ratio'] = odis.risque_fermeture/odis.ecoles_ct

#Scaling with PowerTransformer so that 
# 1. outliers don't impact too much the end result
# 2. all scores are normaly distributed and centered around 0
#pt = preprocessing.PowerTransformer()
t = preprocessing.QuantileTransformer(output_distribution="uniform")
odis['met_scaled'] = t.fit_transform(odis[['met_ratio']].fillna(0)).round(3)
odis['met_tension_scaled'] = t.fit_transform(odis[['met_tension_ratio']].fillna(0)).round(3)
odis['svc_incl_scaled'] = t.fit_transform(odis[['svc_incl_ratio']].fillna(0)).round(3)
odis['log_vac_scaled'] = t.fit_transform(odis[['log_vac_ratio']].fillna(0)).round(3)
odis['log_5p_scaled'] = t.fit_transform(odis[['log_5p_ratio']].fillna(0)).round(3)
odis['classes_ferm_scaled'] = t.fit_transform(odis[['risque_fermeture_ratio']].fillna(0)).round(3)
odis['pol_scaled'] = odis[['pol_num']].astype('float').round(3)

#Adding a category for each score that will be used to assign weights for the weighted avg
scores_cat = pd.DataFrame([
    {'score':'met_scaled','score_name':'Taux Besoin Emploi','cat':'emploi'},
    {'score':'met_tension_scaled','score_name':'Taux Besoin Emploi en Tension','cat':'emploi'},
    {'score':'svc_incl_scaled','score_name':'Taux Services Inclusion','cat':'soutien'},
    {'score':'log_vac_scaled','score_name':'Taux Logements Vacants','cat':'logement'},
    {'score':'log_5p_scaled','score_name':'Taux Grandes Résidences Principales','cat':'logement'},
    {'score':'classes_ferm_scaled','score_name':'Taux Classe à Risque de Fermeture','cat':'education'},
    {'score':'pol_scaled','score_name':'Couleur Politique','cat':'soutien'},
    ])
# Subject preferences weighted score computation
# prefs = {
#     'emploi':2,
#     'logement':1,
#     'education':1,
#     'soutien':1,
#     'mobilité':0,
#     'commune_actuelle':'33281',
#     'loc_distance_km':10,
#     'codes_metiers':{
#         'codes_metiers_adulte1':['S1X40','J0X33','A1X41'],
#         'codes_metiers_adulte2':['T4X60','T2A60']
#     },
#     'codes_formations':{
#         'codes_formations_adulte1':['320'],
#         'codes_formations_adulte2':['333','100']
#     },
#     'age_enfants':{
#         'age_enfant1':4,
#         'age_enfant2':10,
#         'age_enfant3':None,
#         'age_enfant4':None,
#         'age_enfant5':None
#     }
# }
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
#add_distance_to_current_loc(odis, current_codgeo=LOC_COMMUNE_ACTUELLE)
#Adding score specific to subject looking for a job identified as en besoin
def codes_match(df, codes_list):
    #returns a list of codfaps that matches
    if df is None:
        return []
    return list(set(df.tolist()).intersection(set(codes_list)))

def fap_names_lookup(df):
    return list(codfap_index[codfap_index['Code FAP 341'].isin(df)]['Intitulé FAP 341'])

def compute_subject_specific_scores(df, scores_cat, subject_pref): 
    # Let's create subject-specific scores
    
    #For each adult we look for jobs categories that match what is needed
    i=1
    for adult in subject_pref['codes_metiers']:
        df['met_match_codes_adult'+str(i)] = df.be_codfap_top.apply(codes_match, codes_list=subject_pref['codes_metiers'][adult])
        df['met_match_adult'+str(i)] = df['met_match_codes_adult'+str(i)].apply(len)
        df['met_match_adult'+str(i)+'_scaled'] = t.fit_transform(df[['met_match_adult'+str(i)]].fillna(0))
        i+=1

    j=1
    for adult in subject_pref['codes_formations']:
        df['form_match_codes_adult'+str(j)] = df.codes_formations.apply(codes_match, codes_list=subject_pref['codes_formations'][adult])
        df['form_match_adult'+str(j)] = df['form_match_codes_adult'+str(j)].apply(len)
        df['form_match_adult'+str(j)+'_scaled'] = t.fit_transform(df[['form_match_adult'+str(j)]].fillna(0))
        j+=1

    # We compute the distance from the current location 
    df['reloc_dist_scaled'] = (1-df['dist_current_loc']/(subject_pref['loc_distance_km']*1000))
    df['reloc_epci_scaled'] = np.where(df['epci_code'] == df[df.codgeo == subject_pref['commune_actuelle']]['epci_code'].iloc[0],1,0)
    
    #Let's not forget to categorize these new scores to the existing score_cat index if it doesnt exist
    #if (scores_cat.score != 'met_match_scaled').all():
    scores_cat_subject = pd.DataFrame([
        {'score':'met_match_adult1_scaled','score_name':'Match compétences et Besoin Emploi Adult 1','cat':'emploi'},
        {'score':'met_match_adult2_scaled','score_name':'Match compétences et Besoin Emploi Adult 2','cat':'emploi'},
        {'score':'form_match_adult1_scaled','score_name':'Match compétences et Centres de formation','cat':'emploi'},
        {'score':'form_match_adult2_scaled','score_name':'Match compétences et Centres de formation','cat':'emploi'},
        {'score':'reloc_dist_scaled','score_name':'Distance de la localisation actuelle','cat':'mobilité'},
        {'score':'reloc_epci_scaled','score_name':'Même EPCI que la localisation actuelle','cat':'mobilité'}
        ])
    scores_cat_subject = pd.concat([scores_cat, scores_cat_subject])
    return scores_cat_subject
# Let's create subject-specific scores
#scores_cat_subject = compute_subject_specific_scores(odis, scores_cat=scores_cat)
# Filtering dataframe based on subject distance preference (to save on compute time later on)
def filter_loc_by_distance(df, distance):
    return odis[odis.dist_current_loc < distance * 1000]
#odis_search = filter_loc_by_distance(odis, distance=LOC_DISTANCE_KM)
def adding_score_voisins(df_search, df_source):
    binome_columns = ['codgeo','libgeo','codgeo_voisins','polygon','epci_code','epci_nom']+[col for col in df_source.columns if col.startswith('met_match_codes')]+[col for col in df_source.columns if col.endswith('_scaled')] 

    # Adds itself to list of voisins = monome case
    # Note: this code triggers the SettingWithCopyWarning but I don't know how to fix it...
    df_search = df_search.copy()
    df_search.codgeo_voisins = df_search.apply(lambda x: np.append(x.codgeo_voisins, x.codgeo), axis=1)

    # Explodes the dataframe to have a row for each voisins + itself
    df_search['codgeo_voisins_copy'] = df_search['codgeo_voisins']
    df_search_exploded = df_search.explode('codgeo_voisins_copy')
    
    # For each commune (codgeo) in search area (df_search) we add all its voisin's scores
    odis_search_exploded = pd.merge(df_search_exploded, df_source[binome_columns].add_suffix('_binome'), left_on='codgeo_voisins_copy', right_on='codgeo_binome', how='left')
    
    # Adds a column to identify binomes vs monomes + cleanup
    odis_search_exploded['binome'] = odis_search_exploded.apply(lambda x: False if x.codgeo == x.codgeo_binome else True, axis=1)
    odis_search_exploded.drop(columns={'codgeo_voisins_copy'}, inplace=True)
    
    return odis_search_exploded
#odis_exploded = adding_score_voisins(odis_search, odis)
#This was my first attempt with .apply(). It works but super slow... 
def compute_cat_scores_old(df, scores_cat):
    for cat in set(scores_cat.cat):
        cat_scores_list = []
        cat_scores_list_binome = []
        for score in scores_cat[scores_cat.cat==cat].score.to_list():
            #print(cat + ' | ' +score+ ' | ' +str(df.loc[score]))
            cat_scores_list += [df.loc[score]]
            cat_scores_list_binome += [df.loc[score+'_binome']]
        df[cat+'_cat_score'] = np.mean(cat_scores_list)
        df[cat+'_cat_score_binome'] = np.mean(cat_scores_list_binome)
    return df
def compute_cat_scores(df, scores_cat):
    df = df.copy()
    for cat in set(scores_cat.cat):
        cat_scores_indices = scores_cat[scores_cat['cat'] == cat]['score'].tolist()
        cat_scores_indices_binome = [score+'_binome' for score in cat_scores_indices]

        # Efficiently select all relevant rows at once
        cat_scores_df = df[cat_scores_indices]
        cat_scores_binome_df = df[cat_scores_indices_binome]
        
        # Calculate the mean of the selected rows
        df[cat + '_cat_score'] = 100 * cat_scores_df.astype(float).mean(axis=1).round(3)
        df[cat + '_cat_score_binome'] = 100 * cat_scores_binome_df.astype(float).mean(axis=1).round(3)

    return df
#odis_exploded = odis_exploded.apply(compute_cat_scores,scores_cat=scores_cat_subject, axis=1)
#COM_LIMITROPHE_PENALTY = 0.1 #décote de 10% pour les communes limitrophes vs commune cible

def compute_binome_score(df, binome_penalty, weights):
    scores_col = [col for col in df.columns if col.endswith('_cat_score')]
    max_scores = pd.DataFrame()
    
    for col in scores_col:
        cat_weight = weights[col.split('_')[0]]
        max_scores[col] = cat_weight * np.where(
            df[col] >= (1-binome_penalty)*df[col+'_binome'],
            df[col],
            (1-binome_penalty)*df[col+'_binome']
            )
    
    return max_scores.mean(axis=1).round(1)
# We provide the scores columns as a kwarg to compute faster
#odis_exploded['weighted_score'] = compute_binome_score(odis_exploded, binome_penalty=COM_LIMITROPHE_PENALTY)
def monome_cleanup(df):
    if df.loc['binome'] == False:
        for col in df.index:
            if col.endswith('_binome'):
                df.loc[col] = None
    return df

def best_score_compute(df):
    #Keeping the best (top #1) monome or binome result for each commune
    best = df.sort_values('weighted_score', ascending=False).groupby('codgeo').head(1)
    #Cleanup redundant data in the monome cases
    best = best.apply(monome_cleanup, axis=1)
    return best

#odis_search_best = best_score_compute(odis_exploded)
#Main function that aggregates most of the above in one sequence
def compute_odis_score(df,scores_cat, prefs):
    # Note: we consider that the dataframe already has all the commune-specific criteria scores
    # Note2: the prefs dict has all the subject preferences
    
    # We add the distance between each commune and the one set as where the subject currently lives and filter based on distance pref
    add_distance_to_current_loc(odis, current_codgeo=prefs['commune_actuelle'])

    # We compute the subject specific scores
    scores_cat = compute_subject_specific_scores(odis, scores_cat=scores_cat, subject_pref=prefs)
    
    # We filter by distance to reduce the compute cost on a smaller odis_search dataframe
    odis_search = filter_loc_by_distance(odis, distance=prefs['loc_distance_km'])

    # We add the criteria scores for all neighbor communes forming monomes and binomes
    odis_exploded = adding_score_voisins(odis_search, odis)

    # We compute the category scores for both the target and the binome
    odis_exploded = compute_cat_scores(odis_exploded, scores_cat=scores_cat)
    
    # We provide the scores columns as a parameter to compute faster
    #scores_col = [col for col in odis_exploded.columns if col.endswith('_cat_score')]
    
    # We computing the final weighted score for all commune<->voisin combinations
    odis_exploded['weighted_score'] = compute_binome_score(
        odis_exploded,
        binome_penalty=prefs['binome_penalty'],
        weights=prefs
        )
    
    # We keep best monome or binome for each commune 
    odis_search_best = best_score_compute(odis_exploded)

    return odis_search_best, scores_cat
def produce_pitch(df, scores_cat):
    pitch_lines = []
    pitch_lines += [df.loc['libgeo'] +'dans l\'EPCI: '+ df.loc['epci_nom']]
    pitch_lines += ['Le score est de: '+str(df.loc['weighted_score'])]
    if df.loc['binome']:
        pitch_lines += ['Ce score est obtenu en binome avec la commune '+df.loc['libgeo_binome']]
        if df.loc['epci_code'] != df.loc['epci_code_binome']:
            pitch_lines += ['Cette commune est située dans l\'EPCI: '+df.loc['epci_nom_binome']]
    else:
        pitch_lines += ['Ce score est obtenu sans commune binôme']

    
    #Adding the top contributin criterias
    crit_scores_col = [col for col in df.index if '_scaled' in col]#col.endswith('_scaled')]
    df_sorted=df[crit_scores_col].dropna().sort_values(ascending=False)
    for i in range(0, 5):
        # print(df_sorted.index[i])
        # if df_sorted.index[i].endswith('binome'):
        #     nom_critere = scores_cat.loc[df_sorted.index[i][:-7]]['score_name']
        # else:
        #     nom_critere = scores_cat.loc[df_sorted.index[i]]['score_name']
        pitch_lines += ['Le critère #'+str(i+1)+' est: '+df_sorted.index[i]+' avec un score de: '+str(df_sorted.iloc[i])]

    #Adding the matching job families if any
    metiers_col = [col for col in df.index if col.startswith('met_match_codes')]
    for metiers_adultx in metiers_col:
        matched_codfap_names = list(codfap_index[codfap_index['Code FAP 341'].isin(df.loc[metiers_adultx])]['Intitulé FAP 341'])
        if len(matched_codfap_names) == 1:
            pitch_lines += ['La famille de métiers '+ str(matched_codfap_names) +' est rechechée'] 
        elif len(matched_codfap_names) >= 1:
            pitch_lines += ['Les familles de métiers '+ str(matched_codfap_names) +' sont rechechées']
    
    return pitch_lines
    #for line in pitch_lines:
    #    print(line)
def produce_html_tooltip(df):
    tooltip = '<div>'
    tooltip += '<div><strong>'+df.loc['libgeo'] +'</strong> dans l\'EPCI: '+ df.loc['epci_nom']+'</div>'
    tooltip += '<div>'+'Le score est de: <strong>'+str(df.loc['weighted_score'])+'</strong></div>'
    tooltip += '<div style="background-color:white;height:20px;width:200px;">'
    tooltip += '<div style="background-color:green;height:20px;width:'+str(df.loc['weighted_score']*2)+'px;"></div></div>'
    
    if df.loc['binome']:
        tooltip += '<div>'+'Ce score est obtenu en binome avec la commune '+df.loc['libgeo_binome']+'</div>'
        if df.loc['epci_code'] != df.loc['epci_code_binome']:
            tooltip += '<div>'+'Cette commune est située dans l\'EPCI: '+df.loc['epci_nom_binome']+'</div>'
    else:
        tooltip += '<div>'+'Ce score est obtenu sans commune binôme'+'</div>'

    
    #Adding the top contributin criterias
    crit_scores_col = [col for col in df.index if '_scaled' in col]#col.endswith('_scaled')]
    cat_scores_col = [col for col in df.index if col.endswith('_cat_score')]
    df_sorted=df[cat_scores_col].dropna().sort_values(ascending=False)
    for i in range(0, 5):
        tooltip += '<div>'+'Le critère #'+str(i+1)+' est: '+df_sorted.index[i]+' avec un score de: '+str(df_sorted.iloc[i])+'</div>'

    #Adding the matching job families if any
    matched_codfap_names = list(codfap_index[codfap_index['Code FAP 341'].isin(df.loc['met_match_codes'])]['Intitulé FAP 341'])
    if len(matched_codfap_names) == 1:
        tooltip += '<div>'+'La famille de métiers '+ str(matched_codfap_names) +' est rechechée'+'</div>' 
    elif len(matched_codfap_names) >= 1:
        tooltip += '<div>'+'Les familles de métiers '+ str(matched_codfap_names) +' sont rechechées'+'</div>'
    tooltip += '</div>'
    
    return tooltip
# THIS SHOULD BE THE END OF JUPYTER NOTEBOOK EXPORT
