#!/usr/bin/env python
# coding: utf-8

# ## Intro

# ### Scoring Logic Description
# 
# 1. We start by importing the odis dataframe from a CSV that includes all the relevant datapoint to score and display data
# 2. We compute the scores for each criteria specific to the commune (independant from subject)
# 3. We compute the scores for each criteria specific to the subject (dependand from both subject and commune) 
# 4. We identify all commune<->neighbour pairs (binômes) for each commune within search radius
# 5. We compute category scores (emploi, logement, education etc...) as an average of the all the scores for a given category
# 6. For each commune we compare the commune and neighbour category scores and weighted the highest one with category weights defined by subject and then keep the best weighted score for each commune
# 8. We display result in on a map

# In[1]:


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


# ## 1. Fetching key indicators from ODIS source file

# In[2]:


df = gpd.read_parquet('../csv/odis_april_2025_jacques.parquet')


# In[3]:


odis = gpd.GeoDataFrame(df)
odis.set_geometry(odis.polygon, inplace=True)
odis = odis[~odis.polygon.isna()]


# In[4]:


#Later we need the code FAP <-> FAP Name used to classify jobs
codfap_index = pd.read_csv('../csv/dares_nomenclature_fap2021.csv', delimiter=';')


# ## 2. Criteria Scoring for each commune

# In[5]:


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


# ## 3. Criterias specific to subject

# In[6]:


#Subject Default Weights & Preferences
prefs = [
    ['emploi',1],
    ['logement',1],
    ['education',1],
    ['soutien',1],
    ['mobilité',1],
    ['commune_actuelle','31555'],
    ['loc_distance_km',20],
    ['code_metiers',['T4X60','T2A60']]
]

def set_subject_preferences(prefs):
    return pd.DataFrame(prefs, columns=['pref','value']).set_index('pref')

subject_pref = set_subject_preferences(prefs)
LISTE_CODE_METIERS = subject_pref.loc['code_metiers'].value
LOC_COMMUNE_ACTUELLE = subject_pref.loc['commune_actuelle'].value
LOC_DISTANCE_KM= subject_pref.loc['loc_distance_km'].value


# In[7]:


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


# In[8]:


#add_distance_to_current_loc(odis, current_codgeo=LOC_COMMUNE_ACTUELLE)


# In[9]:


#Adding score specific to subject looking for a job identified as en besoin
def code_metiers_match(df, liste_codfap):
    #returns a list of codfaps that matches
    return list(set(df.tolist()).intersection(set(liste_codfap)))

def fap_names_lookup(df):
    return list(codfap_index[codfap_index['Code FAP 341'].isin(df)]['Intitulé FAP 341'])

def compute_subject_specific_scores(df, scores_cat): 
    # Let's create subject-specific scores
    df['met_match_codes'] = df.be_codfap_top5.apply(code_metiers_match, liste_codfap=LISTE_CODE_METIERS) 
    df['met_match'] = df['met_match_codes'].apply(len)
    df['met_match_scaled'] = t.fit_transform(df[['met_match']].fillna(0)).round(3)
    #need to correct the statement below as we can get a negative value for a binome
    df['reloc_dist_scaled'] = (1-df['dist_current_loc']/(LOC_DISTANCE_KM*1000)).round(3)
    df['reloc_epci_scaled'] = np.where(df['epci_code'] == df[df.codgeo == LOC_COMMUNE_ACTUELLE]['epci_code'].iloc[0],1,0)

    #Let's not forget to categorize these new scores to the existing score_cat index if it doesnt exist
    #if (scores_cat.score != 'met_match_scaled').all():
    scores_cat_subject = pd.DataFrame([
        {'score':'met_match_scaled','score_name':'Match compétences et Besoin Emploi','cat':'emploi'},
        {'score':'reloc_dist_scaled','score_name':'Distance de la localisation actuelle','cat':'mobilité'},
        {'score':'reloc_epci_scaled','score_name':'Même EPCI que la localisation actuelle','cat':'mobilité'}
        ])
    scores_cat_subject = pd.concat([scores_cat, scores_cat_subject])
    return scores_cat_subject


# In[10]:


# Let's create subject-specific scores
#scores_cat_subject = compute_subject_specific_scores(odis, scores_cat=scores_cat)


# ## 4. Distance filter + Gathering nearby Communes Scores

# In[11]:


# Filtering dataframe based on subject distance preference (to save on compute time later on)
def filter_loc_by_distance(df, distance):
    return odis[odis.dist_current_loc < distance * 1000]


# In[12]:


#odis_search = filter_loc_by_distance(odis, distance=LOC_DISTANCE_KM)


# In[13]:


def adding_score_voisins(df_search, df_source):
    binome_columns = ['codgeo','libgeo','codgeo_voisins','polygon','met_match_codes','epci_code','epci_nom']+[col for col in df_source.columns if col.endswith('_scaled')] 

    # Adds itself to list of voisins = monome case
    # Note: this code triggers the SettingWithCopyWarning but I don't know how to fix it...
    df_search = df_search.copy()
    df_search.codgeo_voisins = df_search.apply(lambda x: np.append(x.codgeo_voisins, x.codgeo), axis=1)

    # Explodes the dataframe to have a row for each voisins + itself
    df_search_exploded = df_search.explode('codgeo_voisins')

    # For each commune (codgeo) in search area (df_search) we add all its voisin's scores
    odis_search_exploded = pd.merge(df_search_exploded, df_source[binome_columns].add_suffix('_binome'), left_on='codgeo_voisins', right_on='codgeo_binome', how='left')

    # Adds a column to identify binomes vs monomes + cleanup
    odis_search_exploded['binome'] = odis_search_exploded.apply(lambda x: False if x.codgeo == x.codgeo_binome else True, axis=1)
    odis_search_exploded.drop(columns={'codgeo_voisins'}, inplace=True)

    return odis_search_exploded


# In[14]:


#odis_exploded = adding_score_voisins(odis_search, odis)


# ## 5. Computing category scores

# In[15]:


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


# In[16]:


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


# In[17]:


#odis_exploded = odis_exploded.apply(compute_cat_scores,scores_cat=scores_cat_subject, axis=1)


# ## 6. Final Binome Score Weighted

# In[18]:


COM_LIMITROPHE_PENALTY = 0.1 #décote de 10% pour les communes limitrophes vs commune cible

def compute_binome_score(df, binome_penalty, weights):
    scores_col = [col for col in df.columns if col.endswith('_cat_score')]
    max_scores = pd.DataFrame()

    for col in scores_col:
        cat_weight = weights.loc[col.split('_')[0], 'value']
        max_scores[col] = cat_weight * np.where(
            df[col] >= (1-binome_penalty)*df[col+'_binome'],
            df[col],
            (1-binome_penalty)*df[col+'_binome']
            )

    return max_scores.mean(axis=1).round(1)


# In[19]:


# We provide the scores columns as a kwarg to compute faster
#odis_exploded['weighted_score'] = compute_binome_score(odis_exploded, binome_penalty=COM_LIMITROPHE_PENALTY)


# In[20]:


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


# In[21]:


#Main function that aggregates most of the above in one sequence
def compute_odis_score(df,scores_cat, prefs):
    # Note: we consider that the dataframe already has all the commune-specific criteria scores

    # We set variables and scoring weight matrix for the specific subject
    subject_pref = set_subject_preferences(prefs)
    LISTE_CODE_METIERS = subject_pref.loc['code_metiers'].value
    LOC_COMMUNE_ACTUELLE = subject_pref.loc['commune_actuelle'].value
    LOC_DISTANCE_KM = subject_pref.loc['loc_distance_km'].value

    # We add the distance between each commune and the one set as where the subject currently lives and filter based on distance pref
    add_distance_to_current_loc(odis, current_codgeo=LOC_COMMUNE_ACTUELLE)

    # We compute the subject specific scores
    scores_cat_subject = compute_subject_specific_scores(odis, scores_cat=scores_cat)

    # We filter by distance to reduce the compute cost on a smaller odis_search dataframe
    odis_search = filter_loc_by_distance(odis, distance=LOC_DISTANCE_KM)

    # We add the criteria scores for all neighbor communes forming monomes and binomes
    odis_exploded = adding_score_voisins(odis_search, odis)

    # We compute the category scores for both the target and the binome
    odis_exploded = compute_cat_scores(odis_exploded, scores_cat=scores_cat_subject)

    # We provide the scores columns as a parameter to compute faster
    #scores_col = [col for col in odis_exploded.columns if col.endswith('_cat_score')]

    # We computing the final weighted score for all commune<->voisin combinations
    odis_exploded['weighted_score'] = compute_binome_score(
        odis_exploded,
        binome_penalty=COM_LIMITROPHE_PENALTY,
        weights=subject_pref
        )

    # We keep best monome or binome for each commune 
    odis_search_best = best_score_compute(odis_exploded)

    return odis_search_best


# ## 7.Exploring

# In[22]:


# Subject preferences weighted score computation
# prefs = [
#     ['emploi',2],
#     ['logement',1],
#     ['education',1],
#     ['soutien',1],
#     ['mobilité',0],
#     ['commune_actuelle','33281'],
#     ['loc_distance_km',50],
#     ['code_metiers',['S1X40','J0X33','A1X41']]
# ]

# Weight paramaters
# COM_LIMITROPHE_PENALTY = 0.8 #0.1 = décote de 10% pour les communes limitrophes vs commune cible

# Computing Weighted Scores given a source dataframe with geo info and a scoring categorisation
# odis_search_best = compute_odis_score(odis, scores_cat=scores_cat, prefs=prefs)

# Showing results on an interactive map
# cols_to_show = (['codgeo','libgeo','weighted_score','binome','libgeo_binome','dist_current_loc','polygon',
#                  'met_match_codes','met_match_codes_binome']
#                 +[col for col in odis_search_best.columns if '_cat_score' in col])
#odis_search_best[cols_to_show].explore('weighted_score', popup=True)
#odis_search_best.plot('weighted_score')


# In[27]:


# odis_search_best[['epci_nom','polygon']].explore('epci_nom')


# In[24]:


# odis_search_best[cols_to_show].sort_values('weighted_score', ascending=False).head(1)


# ## 8.Generating Narrative

# Here we want to generate a 'human readable' explanation about why scored high a given location.
# Things to show:
# - Target commune name and EPCI
# - Weighted Score
# - If Binome, show the binome and EPCI if different from target
# - Show top 3 criterias target (weighted ?) 
# - Show top 3 criterias for binome (weighted ?)

# In[258]:


def produce_pitch(df):
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
        pitch_lines += ['Le critère #'+str(i+1)+' est: '+df_sorted.index[i]+' avec un score de: '+str(df_sorted.iloc[i])]

    #Adding the matching job families if any
    matched_codfap_names = list(codfap_index[codfap_index['Code FAP 341'].isin(df.loc['met_match_codes'])]['Intitulé FAP 341'])
    if len(matched_codfap_names) == 1:
        pitch_lines += ['La famille de métiers '+ str(matched_codfap_names) +' est rechechée'] 
    elif len(matched_codfap_names) >= 1:
        pitch_lines += ['Les familles de métiers '+ str(matched_codfap_names) +' sont rechechées']

    return pitch_lines
    #for line in pitch_lines:
    #    print(line)


# In[259]:


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


# In[260]:


#odis_search_best = odis_search_best.sort_values('weighted_score', ascending=False).head(5)
# odis_search_best['pitch'] = odis_search_best.apply(produce_pitch, axis=1)
# odis_search_best['tooltip'] = odis_search_best.apply(produce_html_tooltip, axis=1)
# #exemple de pitch
# odis_search_best.tooltip.iloc[0]


# ## 9. Export to SuperSet

# In[261]:


def concatenate_strings(row):
  return '{"type": "Feature","geometry":' + shp.to_geojson(row['polygon']) + '}'


# odis_search_best_export = gpd.GeoDataFrame(odis_search_best.copy())
# odis_search_best_export.set_geometry(odis_search_best_export.polygon, crs='EPSG:2154', inplace=True)
# odis_search_best_export.to_crs(epsg=4326, inplace=True)
# odis_search_best_export["polygon_as_json"] = odis_search_best_export.apply(concatenate_strings, axis=1)
# odis_search_best_export.drop(['polygon','polygon_binome'], axis=1, inplace=True)

# cols = ['met_match_codes','met_match_codes_binome','be_codfap_top5','be_libfap_top5','codgeo_voisins_binome','pitch']
# for col in cols:
#     odis_search_best_export[col] = odis_search_best_export[col].apply(lambda x: x.tolist() if type(x) == np.ndarray else x)


# In[262]:


# from sqlalchemy import create_engine, text

# db_host = "localhost"  # Replace with the actual host (e.g., 'superset_db' if in the same Docker network, or 'localhost' if exposed)
# db_port = "5433"  # Replace with the actual port (usually 5432)
# db_user = "superset"  # Replace with the database user (often 'superset')
# db_password = "superset"  # Replace with the database password
# db_name = "examples"  # Replace with the database name (often 'superset')

# engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')


# # In[263]:


# table_name = "odis_stream2_result"  # Choose a name for the table in PostgreSQL
# odis_search_best_export.to_sql(table_name, engine, if_exists='replace', schema='public', index=False)
# sql = text("GRANT SELECT ON odis_stream2_result TO examples")

# with engine.begin() as connection:
#     connection.execute(sql)

# print(f"DataFrame successfully written to table '{table_name}' in the Superset database.")


# Note to myself:
# Après avoir importé les données dans Postgres il faut donner les droits au user 'examples' sur la table
# > docker exec -it superset_db psql -h superset_db -p 5432 -U superset -d examples
# 
# > GRANT SELECT ON odis_stream2_result TO examples;
# 
# > GRANT USAGE ON SCHEMA public TO examples;
# 

# 

# 
