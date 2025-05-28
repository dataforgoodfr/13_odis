from time import gmtime, strftime, sleep, time
print('############################################')

def performance_tracker(t, text, timer_mode):
    if timer_mode:
        print(str(round(time()-t,2))+'|'+text)
        return time()
t = time()
timer_mode = True

t = performance_tracker(t, 'Start Import', timer_mode)
# Notebook Specific
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
from scipy import stats
import shapely as shp
from shapely.wkt import loads
from shapely.geometry import Polygon
from sklearn import preprocessing

# Streamlit App Specific
import streamlit as st
import folium as flm
from streamlit_folium import st_folium
from branca.colormap import linear
import hashlib
from odisscoring import compute_odis_score, init_loading_datasets#, produce_pitch_markdown 

t = performance_tracker(t, 'End Import', timer_mode)

st.set_page_config(layout="wide", page_title='Odis Stream2 Prototype')

### INIT OF THE STREAMLIT APP ###

def session_states_init():
    if "odis" not in st.session_state:
        st.session_state["odis"] = None
    if "codfap_index" not in st.session_state:
        st.session_state["codfap_index"] = None
    if "codformations_index" not in st.session_state:
        st.session_state["codformations_index"] = None
    if "annuaire_ecoles" not in st.session_state:
        st.session_state["annuaire_ecoles"] = None
    if "scores_cat" not in st.session_state:
        st.session_state["scores_cat"] = None

    # Local variables
    if "processed_gdf" not in st.session_state:
        st.session_state["processed_gdf"] = None
    if "selected_geo" not in st.session_state:
        st.session_state["selected_geo"] = None
    if "fg_list" not in st.session_state:
        st.session_state["fg_list"] = []
    if 'afficher_ecoles' not in st.session_state:
        st.session_state['afficher_ecoles'] = None
    if 'fg_extras_dict' not in st.session_state:
        st.session_state["fg_extras_dict"] = {}
    if 'fg_dict_key' not in st.session_state:
        st.session_state["fg_dict_key"] = 'Scores'
    if 'pitch' not in st.session_state:
        st.session_state["pitch"] = []
    if 'fg_dict' not in st.session_state:
        st.session_state["fg_dict"] = {}
    if 'prefs' not in st.session_state:
        st.session_state["prefs"] = {}
    if "fg_ecoles" not in st.session_state:
        st.session_state["fg_ecoles"] = None

# This @st.cache_resource dramatically improves performance of the app
@st.cache_resource
def init_datasets():
    # We load all the datasets
    odis, scores_cat, codfap_index, codformations_index, annuaire_ecoles = init_loading_datasets(ODIS_FILE, SCORES_CAT_FILE, METIERS_FILE, FORMATIONS_FILE, ECOLES_FILE)
    
    coddep_set = sorted(set(odis['dep_code']))
    depcom_df = odis[['dep_code','libgeo']].sort_values('libgeo')
    codgeo_df = odis[['dep_code','libgeo','codgeo']]
    libfap_set = sorted(set(codfap_index['Intitulé FAP 341']))
    libform_set = sorted(set(codformations_index['libformation']))
    
    return odis, codfap_index, codformations_index, annuaire_ecoles, scores_cat, coddep_set, depcom_df, codgeo_df, libfap_set, libform_set

@st.cache_data
def compute_score(_df, scores_cat, prefs):
    return compute_odis_score(_df, scores_cat, prefs)

def set_prefs():
    prefs = {
        'emploi':poids_emploi,
        'logement':poids_logement,
        'education':poids_education,
        'soutien':poids_soutien,
        'mobilité':poids_mobilité,
        'commune_actuelle':commune_codgeo,
        'loc_distance_km':loc_distance_km,
        'codes_metiers':{
            'codes_metiers_adulte1':liste_metiers_adult[0],
            'codes_metiers_adulte2':liste_metiers_adult[1]
        },
        'codes_formations':{
            'codes_formations_adulte1':liste_formations_adult[0],
            'codes_formations_adulte2':liste_formations_adult[1]
        },
        'age_enfants':{
            'age_enfant1':liste_age_enfants[0],
            'age_enfant2':liste_age_enfants[1],
            'age_enfant3':liste_age_enfants[2],
            'age_enfant4':liste_age_enfants[3],
            'age_enfant5':liste_age_enfants[4]
        },
        'binome_penalty':penalite_binome
    }
    # Sample Data
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
    #         'codes_formations_adulte1':[423],
    #         'codes_formations_adulte2':[315,100]
    #     },
    #     'age_enfants':{
    #         'age_enfant1':4,
    #         'age_enfant2':10,
    #         'age_enfant3':None,
    #         'age_enfant4':None,
    #         'age_enfant5':None
    #     },
    #     'binome_penalty':0.1
    # }
    st.session_state["prefs_current"] = prefs
    return prefs

def load_results(df, scores_cat): 
    print(' <---------------------> CLICK <--------------------->')
    prefs = set_prefs() # We update the prefs with the latest inputs
    # Computing Weighted Scores given a source dataframe with geo info and a scoring categorisation
    odis_scored = compute_score(
        _df=df,
        scores_cat=scores_cat,
        prefs=prefs
        )
    odis_scored = odis_scored.sort_values('weighted_score', ascending=False).reset_index(drop=True)
    odis_scored.to_crs(epsg=4326, inplace=True)
    selected_geo = odis_scored[odis_scored.codgeo == prefs['commune_actuelle']]
    st.session_state['prefs'] = prefs
    st.session_state['scores_cat'] = scores_cat
    st.session_state['processed_gdf'] = odis_scored
    st.session_state['selected_geo'] = selected_geo
    st.session_state['afficher_ecoles'] = False

def styling_communes(feature, style):
    score_weights_dict = st.session_state["processed_gdf"].set_index("codgeo")["weighted_score"]
    colormap = linear.YlGn_09.scale(st.session_state["processed_gdf"]['weighted_score'].min(), st.session_state["processed_gdf"]['weighted_score'].max())
    
    if style == 'style_score':
        style_to_use = {
            #"fillColor": "#1100f8",
            "fillColor": colormap(score_weights_dict[feature]),
            "color": "#535353",
            "fillOpacity": 0.8,
            "weight": 1,
        }
    if style == 'style_target':
        style_to_use = {
            "color": "red",
            "fillOpacity": 0,
            "weight": 3,
            "opacity": 1,
            "html":'<div style="width: 10px;height: 10px;border: 1px solid black;border-radius: 5px;background-color: orange;">1</div>'
            # html='<div style="font-size: 24pt">%s</div>' % text,
        }

    if style == 'style_binome':
        style_to_use = {
            "color": "red",
            "fillOpacity": 0,
            "weight": 2,
            "opacity": 1,
            "dashArray": "5, 5",
        }
    colors = ["orange", "yellow", "green", "blue"]
    if style == 'ecoles':
        style_to_use = {
            "radius": 8,
            "fillColor": colors[feature['properties']['type_etablissement']],
        }
    return style_to_use

@st.cache_data
def add_commune_to_map(_df, _fg, style, prefs, index):
    style_to_use = styling_communes(feature=_df.codgeo.iloc[0], style=style)
    commune_json = flm.GeoJson(data=_df, style=style_to_use)
    _fg.add_child(commune_json)
    return _fg
    #fg.add_child(flm.Tooltip(str(index+1)))

def result_highlight(row, index):
    fg_dict_key = "Scores+top"+str(index+1)
    st.session_state['pitch'] = produce_pitch_markdown(row, prefs=st.session_state["prefs"], scores_cat=st.session_state['scores_cat'], codfap_index=codfap_index, codformations_index=codformations_index)
    st.session_state['fg_dict_key'] = "Scores+top"+str(index+1)

def build_top_results(_df, prefs):
    for index, row in _df.head(5).iterrows():
        title = f"Top {index+1}: {row.libgeo} (en binôme avec {row.libgeo_binome})" if row.binome else f"Top {index+1}: {row.libgeo} (seule)"
        if st.button(
            title,
            on_click=result_highlight,
            kwargs={'row':row, 'index':index},
            use_container_width=True,
            key='button_top'+str(index+1),
            type='secondary'
            ):
            # st.session_state["afficher_ecoles"] = False
            with st.container(border=True):
                st.markdown(st.session_state['pitch'])
    # This is used as a callback so we don't return anything

def produce_pitch_markdown(df, prefs, scores_cat, codfap_index, codformations_index):
    pitch_md = []
    pitch_md.append(f'**{df.loc["libgeo"]}** dans l\'EPCI: {df.loc["epci_nom"]}.  ')
    pitch_md.append(f'Le score TOTO est de: **{df.loc["weighted_score"]:.2f}**.')
    if df.loc['binome']:
        pitch_md.append(f'Ce score est obtenu en binome avec la commune {df.loc["libgeo_binome"]}')
        if df.loc['epci_code'] != df.loc['epci_code_binome']:
            pitch_md.append(f' située dans l\'EPCI: {df.loc["epci_nom_binome"]}')
    else:
        pitch_md.append(f'Ce score est obtenu sans commune binôme')

    
    #Adding the top contributing criterias
    crit_scores_col = [col for col in df.index if '_scaled' in col]#col.endswith('_scaled')]
    
    df_sorted=df[crit_scores_col].dropna().sort_values(ascending=False)
    for i in range(0, 5):
        score = df_sorted.index[i][:-7] if df_sorted.index[i].endswith('_binome') else df_sorted.index[i]
        score_name = scores_cat[scores_cat.score == score]['score_name'].item()
        pitch_md.append(f'- Le critère #{i+1} est: **{score_name}** avec un score de: **{df_sorted.iloc[i]:.2f}**')

    
    #Adding the matching job families if any
    pitch_md.append('\n **Emploi** \n') 
    metiers_col = [col for col in df.index if col.startswith('met_match_codes')]
    matched_codfap_names = []
    for metiers_adultx in metiers_col:
        matched_codfap_names += list(codfap_index[codfap_index['Code FAP 341'].isin(df.loc[metiers_adultx])]['Intitulé FAP 341'])
    matched_codfap_names = set(matched_codfap_names)
    if len(matched_codfap_names) == 0:
        pitch_md.append(f'Aucun des métiers recherchés ne figure dans le Top 10 des métiers à pourvoir sur cette zone.  ')
        pitch_md.append(f'Top 10 des métiers à pourvoir sur cette zone: **{", ".join(df.loc["be_libfap_top"])}**  ')
    if len(matched_codfap_names) == 1:
        pitch_md.append(f'- La famille de métiers **{matched_codfap_names[0]}** est rechechée  ')
    elif len(matched_codfap_names) >= 1:
        pitch_md.append(f'- Les familles de métiers **{", ".join(matched_codfap_names)}** sont rechechées  ')
        
    return "\n".join(pitch_md)#.strip()

@st.cache_data
def show_scoring_results(_df, _fg_dict, _fg_list, prefs):
    fg_results = flm.FeatureGroup(name="Results")
    _fg_list += [fg_results] #Here we keep track of all possibles feature groups to add the the 'All' option in our fg_dict
    _fg_dict['Scores']=[fg_results]
    for index, row in _df.iterrows():
        # This is for the main layer with all the communes colored according to weighted_score
        df_com = _df[_df.index==index][['codgeo','polygon', 'weighted_score']]
        fg_results = add_commune_to_map(_df=df_com, _fg=fg_results, style='style_score', prefs=prefs, index=index)
        # This is to highlight the top 5 Geos and their corresponding binome
        if index < 5: # top5
            fg_name = 'fg_com'+str(index+1)
            fg_name = flm.FeatureGroup(name="Commune Top"+str(index+1))
            _fg_list += [fg_name]
            fg_dict_name="Scores+top"+str(index+1)
            _fg_dict[fg_dict_name]=_fg_dict['Scores']+[fg_name]
            fg_name = add_commune_to_map(_df=df_com, _fg=fg_name, style='style_target', prefs=prefs, index=index)
            #Same thing for corresponding binomes
            df_binome = _df[_df.index==index][['codgeo','polygon_binome', 'weighted_score']]
            df_binome = gpd.GeoDataFrame(df_binome, geometry='polygon_binome', crs='EPSG:2154').to_crs(epsg=4326)
            fg_name = add_commune_to_map(_df=df_binome, _fg=fg_name, style='style_binome', prefs=prefs, index=index)
    return _fg_dict, _fg_list

@st.cache_data
def odis_base_map(_current_geo, prefs):
    # We store the last known prefs in session to avoid reload
    center_loc = [
            _current_geo.centroid.y.iloc[0],
            _current_geo.centroid.x.iloc[0]
        ]
    m = flm.Map(location=center_loc, zoom_start=10)
    return m

@st.cache_data
def filter_ecoles_by_distance(_current_geo, annuaire_ecoles, prefs):
    # we go twice the distance around the selected geo 
    MAX_DISTANCE_M = st.session_state['prefs']['loc_distance_km']*1000
    annuaire_ecoles = gpd.GeoDataFrame(annuaire_ecoles, geometry='geometry', crs='EPSG:4326')
    annuaire_ecoles.to_crs("EPSG:2154", inplace=True)
    current_geo = gpd.GeoDataFrame(_current_geo[['codgeo','polygon']], geometry='polygon')
    current_geo.to_crs("EPSG:2154", inplace=True)

    filtered_ecoles = gpd.sjoin_nearest(
        annuaire_ecoles,
        current_geo,
        how="left",
        max_distance=MAX_DISTANCE_M,
        distance_col="distance_to_current_geo" # Column to store computed distance
    )
    filtered_ecoles=filtered_ecoles[filtered_ecoles['distance_to_current_geo'].notna()]
    return filtered_ecoles

@st.cache_data
def build_local_ecoles_layer(_current_geo, _annuaire_ecoles, prefs):
    category_colors = {
        'Ecole': 'blue',
        'Collège': 'red',
        'Lycée': 'green',
        'default': 'gray' # Fallback color for unexpected categories
        }
    fg_ecoles = flm.FeatureGroup(name="Établissements Scolaires")
    filtered_ecoles = _annuaire_ecoles[_annuaire_ecoles.type_etablissement.isin(['Ecole', 'Collège', 'Lycée'])]
    filtered_ecoles = filter_ecoles_by_distance(_current_geo, filtered_ecoles, prefs)

    for index, row in filtered_ecoles.iterrows():
        ecole = filtered_ecoles[filtered_ecoles.index==index]
        ecole = flm.GeoJson(
            data=ecole,
            tooltip=flm.GeoJsonTooltip(fields=["nom_etablissement", "type_etablissement", "statut_public_prive", "ecole_maternelle"]),
            marker=flm.Circle(radius=250, fillColor=category_colors[ecole['type_etablissement'].iloc[0]], fill_opacity=1.0, opacity=0.0, weight=1),
        )
        fg_ecoles.add_child(ecole)
    print(fg_ecoles)
    return fg_ecoles

def toggle_ecoles(fg):
    if (st.session_state['afficher_ecoles']):
        print('here')
        st.session_state["fg_extras_dict"]['ecoles'] = fg
        # st.write(':large_blue_circle:= Ecoles,:red_circle:= Collèges et :large_green_circle:= Lycées')
    if (st.session_state['afficher_ecoles'] == False) & ('ecoles' in st.session_state["fg_extras_dict"].keys()):
        print('there')
        st.session_state["fg_extras_dict"].pop('ecoles')
    print(st.session_state["fg_extras_dict"])


# Loading and caching Datasets
ODIS_FILE = '../csv/odis_april_2025_jacques.parquet'
SCORES_CAT_FILE = '../csv/odis_scores_cat.csv'
METIERS_FILE = '../csv/dares_nomenclature_fap2021.csv'
FORMATIONS_FILE = '../csv/index_formations.csv'
ECOLES_FILE = '../csv/annuaire_ecoles_france_mini.parquet'

t = performance_tracker(t, 'Start Dataset Import', timer_mode)
odis, codfap_index, codformations_index, annuaire_ecoles, scores_cat, coddep_set, depcom_df, codgeo_df, libfap_set, libform_set = init_datasets()
t = performance_tracker(t, 'End Dataset Import', timer_mode)

# Load all the session_states if they don't exist yet
session_states_init()

### BEGINNING OF THE STREAMLIT APP ###
t = performance_tracker(t, 'Start App Sidebar', timer_mode)
# Sidebar
st.header("Odis Stream #2 Prototype App")

t = performance_tracker(t, 'Start Top Filters', timer_mode)
#Top filter Form
st.subheader('Préférences de recherche')
with st.form("preferences"):
    tab_mobilite, tab_foyer, tab_emploi, tab_formation, tab_logement, tab_edu, tab_sante, tab_autres, tab_ponderation = st.tabs(['Mobilité', 'Foyer', 'Emploi', 'Formation', 'Logement', 'Education', 'Santé', 'Autres', 'Pondération'])

    #Mobilité
    with tab_mobilite:
        col_left,col_right =st.columns(2)
        with col_left:
            departement_actuel = st.selectbox("Département", coddep_set, index=coddep_set.index('33'))
            communes = depcom_df[depcom_df.dep_code==departement_actuel]['libgeo']
            commune_actuelle = st.selectbox("Commune", communes)
            commune_codgeo = codgeo_df[(codgeo_df.dep_code==departement_actuel) & (codgeo_df.libgeo==commune_actuelle)].codgeo.item()
        with col_right:
            loc_distance_km = st.select_slider("Distance Max Relocalisation (en Km)", options=[0,10,50,100], value=10, disabled=False)
            

    #Foyer
    with tab_foyer:
        col_left,col_right =st.columns(2)
        with col_left:
            nb_adultes = st.radio("Nombre d'adultes", [1,2], index=0)
        with col_right:
            nb_enfants = st.select_slider("Nombre d'enfants", range(0,6), value=1)

    #Metiers
    with tab_emploi:
        liste_metiers_adult=[[],[]]
        for adult in range(0,nb_adultes):
            liste_metiers_adult[adult] = st.multiselect("Métiers ciblés Adulte "+str(adult+1),libfap_set)

    #Formations
    with tab_formation:
        liste_formations_adult=[[],[]]
        for adult in range(0,nb_adultes):
            liste_formations_adult[adult] = st.multiselect("Formations ciblées Adulte "+str(adult+1),libform_set)

    # Logement
    with tab_logement:
        st.radio('Quel logement à court terme',["Chez l'habitant", 'Location', "Logement Social"])
        st.radio('Quel logement à long terme',['Location', "Logement Social"])

    
    #Education
    with tab_edu:
        liste_age_enfants=[[],[],[],[],[]]
        col_left, col_right = st.columns(2)
        if nb_enfants <= 3:
            with col_left:
                for enfant in range(0,nb_enfants):
                    liste_age_enfants[enfant] =st.select_slider('Age enfant '+str(enfant+1),range(0,19), value=None)
        else:
            with col_left:
                for enfant in range(0,3):
                    liste_age_enfants[enfant] =st.select_slider('Age enfant '+str(enfant+1),range(0,19), value=None)
            with col_right:
                for enfant in range(3,nb_enfants):
                    liste_age_enfants[enfant] =st.select_slider('Age enfant '+str(enfant+1),range(0,19), value=None)

    #Santé
    with tab_sante:
        st.radio('upport médical à proximité',["Hopital", 'Maternité', "Centre Addictions"])

    #Autres
    with tab_autres:
        st.text_input('Autres préférences')

    #Poids
    with tab_ponderation:
        poids_emploi = st.select_slider("Pondération Emploi", range(0,5), value=1)
        poids_logement = st.select_slider("Pondération Logement", range(0,5), value=1)
        poids_education = st.select_slider("Pondération Education", range(0,5), value=1)
        poids_soutien = st.select_slider("Pondération Soutien", range(0,5), value=1)
        poids_mobilité = st.select_slider("Pondération Mobilité", range(0,5), value=1)
        penalite_binome = st.select_slider("Décote binôme %", range(0,101), value=25) / 100

    # Bouton Pour lancer le scoring + affichage de la carte
    st.form_submit_button(
    "Afficher la carte" if st.session_state["processed_gdf"] is None else "Mettre à jour la carte",
    on_click=load_results, kwargs={'df':odis, 'scores_cat':scores_cat})



t = performance_tracker(t, 'Start Scoring Results', timer_mode)
# Main 2 sections with results and map
if (st.session_state['processed_gdf'] is not None):
    col_results, col_map = st.columns([1, 1])
    t = performance_tracker(t, 'Start Results Column', timer_mode)
    
    ### Results Column
    with col_results:
        st.subheader("Meilleurs résultats")
        st.markdown('<style>di.st-key-button_top1 .stBUtton button div p  {text-align: left !important;color: red;}</style>',unsafe_allow_html=True)
        if st.button("Afficher tous les meilleurs résultats sur la carte", type='tertiary'):
            st.session_state["fg_dict_key"] = 'All'
        if st.session_state["processed_gdf"] is not None:
            fg_dict_key = build_top_results(st.session_state["processed_gdf"], st.session_state["prefs"])
            

    t = performance_tracker(t, 'Start Map Column', timer_mode)
    ### Map Column
    with col_map:
        st.subheader("Carte Interactive")
        # we have scoring results let's draw the the map
        fg_list = []
        fg_dict = {}
        #scoring results map with shades + highlighted top 5 binomes 
        fg_dict, fg_list = show_scoring_results(st.session_state['processed_gdf'][['codgeo','polygon','polygon_binome','weighted_score']], fg_dict, fg_list, st.session_state['prefs'])
        #We finally add all the created feature_groups to the 'All' option
        fg_dict['All'] = fg_list
        # We now have a fg_dict that looks like this:
        # {
        #     'Scores': fg_results,
        #     'Scores+top1': [fg_results, fg_com1],
        #     'Scores+top2': [fg_results, fg_com2],
        #     'Scores+top3': [fg_results, fg_com3],
        #     'Scores+top4': [fg_results, fg_com4],
        #     'Scores+top5': [fg_results, fg_com5],
        #     'All': [fg_results, fg_com1, fg_com2, fg_com3, fg_com4, fg_com5]
        # }

        # We add additional informational layers
        # We keep the schools in the same academy only
        # See doc here: https://python-visualization.github.io/folium/latest/user_guide/geojson/geojson_marker.html
        t = performance_tracker(t, 'Start Ecoles Display', timer_mode)
        if nb_enfants > 0:
            st.session_state['fg_ecoles'] = build_local_ecoles_layer(st.session_state["selected_geo"], annuaire_ecoles, st.session_state["prefs"])
            st.checkbox('Afficher Ecoles', key='afficher_ecoles', value=False, on_change=toggle_ecoles, kwargs={'fg':st.session_state['fg_ecoles']})

        t = performance_tracker(t, 'End Ecoles Display', timer_mode)

        t = performance_tracker(t, 'Start Map Display', timer_mode)
        # print(fg_dict[st.session_state["fg_dict_key"]]+list(st.session_state["fg_extras_dict"].values()))
        m = odis_base_map(st.session_state['selected_geo'], st.session_state['prefs'])
        st_data = st_folium(
            m,
            feature_group_to_add=fg_dict[st.session_state["fg_dict_key"]]+list(st.session_state["fg_extras_dict"].values()),#fg_dict[st.session_state["fg_dict_key"]],
            width=1000,
            height=800,
            key="odis_scored_map",
            use_container_width=True,
            returned_objects=[],
            # layer_control=flm.LayerControl(collapsed=False)
        )
        t = performance_tracker(t, 'End Map Display', timer_mode)


