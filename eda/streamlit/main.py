from time import gmtime, strftime, sleep
print('############################################')
print(strftime("%Y-%m-%d %H:%M:%S", gmtime())+' Start Import')
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
from odisscoring import compute_odis_score, produce_pitch, init_communes_criterias, init_loading_datasets
print(strftime("%Y-%m-%d %H:%M:%S", gmtime())+' End Import')

st.set_page_config(layout="wide", page_title='Odis Stream2 Prototype')
print(strftime("%Y-%m-%d %H:%M:%S", gmtime())+' Start Page')

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
        st.session_state["fg_list"] = "Scores+top1"
    if 'ecoles_academie' not in st.session_state:
        st.session_state['ecoles_academie'] = None
    if 'center' not in st.session_state:
        st.session_state["center"] = []
    if 'fg_dict_key' not in st.session_state:
        st.session_state["fg_dict_key"] = None
    if 'pitch' not in st.session_state:
        st.session_state["pitch"] = []
    if 'fg_dict' not in st.session_state:
        st.session_state["fg_dict"] = {}
    if 'prefs' not in st.session_state:
        st.session_state["prefs"] = {}
    if "last_object_clicked" not in st.session_state:
        st.session_state["last_object_clicked"] = None

@st.cache_data
def init_datasets(odis_file, metiers_file, formations_file, ecoles_file):
    odis, codfap_index, codformations_index, annuaire_ecoles = init_loading_datasets(odis_file, metiers_file, formations_file, ecoles_file)
    odis, scores_cat = init_communes_criterias(odis)
    coddep_set = sorted(set(odis['dep_code']))
    depcom_df = odis[['dep_code','libgeo']].sort_values('libgeo')
    codgeo_df = odis[['dep_code','libgeo','codgeo']]
    libfap_set = sorted(set(codfap_index['Intitulé FAP 341']))
    libform_set = sorted(set(codformations_index['libformation']))
    return odis, codfap_index, codformations_index, annuaire_ecoles, scores_cat, coddep_set, depcom_df, codgeo_df, libfap_set, libform_set

@st.cache_data
def compute_score(_df, scores_cat, prefs):
    return compute_odis_score(_df, scores_cat, prefs)

def get_csv_hash(file_name):
    with open(file_name, 'rb') as file_to_check:
        # read contents of the file
        data = file_to_check.read()    
        # pipe contents of the file through
        return hashlib.md5(data).hexdigest()

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
    st.session_state["prefs_current"] = prefs
    return prefs

def load_results(df, scores_cat): 
    print(strftime("%Y-%m-%d %H:%M:%S", gmtime())+' <---------------------> CLICK <--------------------->')
    prefs = set_prefs() # We update the prefs with the latest inputs
    # Computing Weighted Scores given a source dataframe with geo info and a scoring categorisation
    print(strftime("%Y-%m-%d %H:%M:%S", gmtime())+' Start Scoring')
    odis_scored = compute_score(
        _df=df,
        scores_cat=scores_cat,
        prefs=prefs
        ).sort_values('weighted_score', ascending=False).reset_index(drop=True)
    odis_scored.to_crs(epsg=4326, inplace=True)
    selected_geo = odis_scored[odis_scored.codgeo == prefs['commune_actuelle']]
    st.session_state['prefs'] = prefs
    st.session_state['processed_gdf'] = odis_scored
    st.session_state['selected_geo'] = selected_geo
    print(strftime("%Y-%m-%d %H:%M:%S", gmtime())+' End Scoring')
    return prefs, odis_scored, selected_geo

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
def add_commune_to_map(_df, _fg, style, index):
    style_to_use = styling_communes(feature=_df.codgeo.iloc[0], style=style)
    # commune_json = flm.GeoJson(data=_df, style_function=lambda feature: styling_communes(feature=feature, style=style))
    commune_json = flm.GeoJson(data=_df, style=style_to_use)
    _fg.add_child(commune_json)
    return _fg
    #fg.add_child(flm.Tooltip(str(index+1)))

def result_highlight(row, index):
    fg_dict_key = "Scores+top"+str(index+1)
    st.session_state['pitch'] = produce_pitch(row, codfap_index=codfap_index)
    st.session_state['fg_dict_key'] = "Scores+top"+str(index+1)

def build_top_results(_df, prefs):
    for index, row in _df.head(5).iterrows():
        title = "Top " + str(index+1) + ': '+row.libgeo+' (en binôme avec '+row.libgeo_binome+')' if row.binome else "Top " + str(index+1) + ': '+row.libgeo+' (seule)'
        fg_dict_key = "Scores" # Default selection 
        if st.button(
            title,
            on_click=result_highlight,
            kwargs={'row':row, 'index':index},
            use_container_width=True,
            key='button_top'+str(index+1),
            type='secondary'
            ):
            with st.container(border=True):
                for row in st.session_state['pitch']:
                    st.write(row)

@st.cache_data
def show_all_results(_df, _fg_dict, _fg_list, prefs):
    fg_results = flm.FeatureGroup(name="Results")
    _fg_list += [fg_results] #Here we keep track of all possibles feature groups to add the the 'All' option in our fg_dict
    _fg_dict['Scores']=fg_results
    for index, row in _df.iterrows():
        # This is for the main layer with all the communes colored according to weighted_score
        df_com = _df[_df.index==index][['codgeo','polygon', 'weighted_score']]
        fg_results = add_commune_to_map(_df=df_com, _fg=fg_results, style='style_score', index=index)
        # This is to highlight the top 5 Geos and their corresponding binome
        if index < 5: # top5
            fg_name = 'fg_com'+str(index+1)
            fg_name = flm.FeatureGroup(name="Commune Top"+str(index+1))
            _fg_list += [fg_name]
            fg_dict_name="Scores+top"+str(index+1)
            _fg_dict[fg_dict_name]=[_fg_dict['Scores']]+[fg_name]
            fg_name = add_commune_to_map(_df=df_com, _fg=fg_name, style='style_target', index=index)
            #Same thing for corresponding binomes
            df_binome = _df[_df.index==index][['codgeo','polygon_binome', 'weighted_score']]
            df_binome = gpd.GeoDataFrame(df_binome, geometry='polygon_binome', crs='EPSG:2154').to_crs(epsg=4326)
            fg_name = add_commune_to_map(_df=df_binome, _fg=fg_name, style='style_binome', index=index)
    return _fg_dict, _fg_list

@st.cache_data
def odis_base_map(_current_geo, prefs):
    # We store the last known prefs in session to avoid reload
    center_loc = [
            _current_geo.centroid.y.iloc[0],
            _current_geo.centroid.x.iloc[0]
        ]
    m = flm.Map(location=center_loc, zoom_start=12)
    return m

@st.cache_data
def show_ecoles():
    category_colors = {
    'Ecole': 'blue',
    'Collège': 'red',
    'Lycée': 'green',
    'default': 'gray' # Fallback color for unexpected categories
    }
    st.session_state['ecoles_academie'] = annuaire_ecoles[
        (annuaire_ecoles.code_academie.astype(int).astype(str) == st.session_state["selected_geo"]['academie_code'].iloc[0])
        & (annuaire_ecoles.type_etablissement.isin(['Ecole', 'Collège','Lycée']))
        ]
    st.session_state['ecoles_academie'] = gpd.GeoDataFrame(st.session_state['ecoles_academie'], geometry='geometry', crs='EPSG:4326')
    st.session_state['ecoles_academie'] = flm.GeoJson(
        st.session_state['ecoles_academie'],
        tooltip=flm.GeoJsonTooltip(fields=["nom_etablissement", "type_etablissement", "statut_public_prive", "ecole_maternelle"]),
        marker=flm.Circle(radius=250, fill_color="orange", fill_opacity=1.0, weight=1),
        style_function=lambda x: {"fillColor": category_colors[x['properties']['type_etablissement']]}
    )
    fg_ecoles = flm.FeatureGroup(name="Établissements Scolaires")
    fg_ecoles.add_child(st.session_state['ecoles_academie'])
        
    afficher_ecoles = st.radio('Afficher Ecoles',['Oui', 'Non'],key='afficher_ecoles', index=1)
    if afficher_ecoles == "Oui":
        fg_dict[st.session_state["fg_list"]].append(fg_ecoles)
        st.write(':large_blue_circle:= Ecoles,:red_circle:= Collèges et :large_green_circle:= Lycées')
    elif fg_ecoles in fg_dict[st.session_state["fg_list"]]:
        fg_dict[st.session_state["fg_list"]].remove(fg_ecoles)
    return fg_dict

# Loading and caching Datasets
ODIS_FILE = '../csv/odis_april_2025_jacques.parquet'
METIERS_FILE = '../csv/dares_nomenclature_fap2021.csv'
FORMATIONS_FILE = '../csv/index_formations.csv'
ECOLES_FILE = '../csv/annuaire_ecoles_france_mini.parquet'

print(strftime("%Y-%m-%d %H:%M:%S", gmtime())+' Start dataset loading ')
odis, codfap_index, codformations_index, annuaire_ecoles, scores_cat, coddep_set, depcom_df, codgeo_df, libfap_set, libform_set = init_datasets(
    odis_file=ODIS_FILE,
    metiers_file=METIERS_FILE,
    formations_file=FORMATIONS_FILE,
    ecoles_file=ECOLES_FILE
    )
print(strftime("%Y-%m-%d %H:%M:%S", gmtime())+' End dataaset loading')

# Load all the session_states if they don't exist yet
session_states_init()

### BEGINNING OF THE STREAMLIT APP ###
print(strftime("%Y-%m-%d %H:%M:%S", gmtime())+' End Init')
# Sidebar
st.sidebar.write("Odis Stream #2 Prototype App")
with st.sidebar:
    st.write('Filtres de recherche')
    # libdep_set, libfap_set, libform_set = get_index_sets(odis, codfap_index, codformations_index)
    #Mobilité
    departement_actuel = st.selectbox("Département", coddep_set, index=coddep_set.index('33'))
    communes = depcom_df[depcom_df.dep_code==departement_actuel]['libgeo']
    commune_actuelle = st.selectbox("Commune", communes)
    commune_codgeo = codgeo_df[(codgeo_df.dep_code==departement_actuel) & (codgeo_df.libgeo==commune_actuelle)].codgeo.item()
    loc_distance_km = st.select_slider("Distance Max Relocalisation", range(0,51), value=5)

    #Foyer
    nb_adultes = st.select_slider("Nombre d'adultes", range(1,3), value=1)
    nb_enfants = st.select_slider("Nombre d'enfants", range(0,6), value=1)

    #Poids
    with st.expander("Pondérations", expanded=False):
        poids_emploi = st.select_slider("Pondération Emploi", range(0,5), value=1)
        poids_logement = st.select_slider("Pondération Logement", range(0,5), value=1)
        poids_education = st.select_slider("Pondération Education", range(0,5), value=1)
        poids_soutien = st.select_slider("Pondération Soutien", range(0,5), value=1)
        poids_mobilité = st.select_slider("Pondération Mobilité", range(0,5), value=1)
        penalite_binome = st.select_slider("Décote binôme %", range(0,101), value=25) / 100
    
#Top filter container
with st.container(border=False):
    st.write('Préférences de recherche')
    col_emploi, col_formation, col_logement = st.columns(3)

    #Metiers
    with col_emploi:
        with st.popover("Emploi", use_container_width=True):
            #Default for list items
            liste_metiers_adult=[[],[]]
            for adult in range(0,nb_adultes):
                liste_metiers_adult[adult] = st.multiselect("Métiers ciblés Adulte "+str(adult+1),libfap_set)

    #Formations
    with col_formation:
        with st.popover("Formations", use_container_width=True):
            liste_formations_adult=[[],[]]
            for adult in range(0,nb_adultes):
                liste_formations_adult[adult] = st.multiselect("Formations ciblées Adulte "+str(adult+1),libform_set)

    # Logement
    with col_logement:
        with st.popover("Logement", use_container_width=True):
            st.radio('Quel logement à court terme',["Chez l'habitant", 'Location', "Logement Social"])
            st.radio('Quel logement à long terme',['Location', "Logement Social"])

    col_edu, col_sante, col_autres = st.columns(3)
    #Education
    with col_edu:
        with st.popover("Éducation", use_container_width=True):
            liste_age_enfants=[[],[],[],[],[],]
            for enfant in range(0,nb_enfants):
                liste_age_enfants[enfant] =st.select_slider('Age enfant '+str(enfant+1),range(0,19), value=None)
            # st.write(liste_age_enfants)

    #Santé
    with col_sante:
        with st.popover("Santé", use_container_width=True):
            st.radio('upport médical à proximité',["Hopital", 'Maternité', "Centre Addictions"])

    with col_autres:
        with st.popover("Autres", use_container_width=True):
            st.text_input('Autres préférences')

# Bouton Pour lancer le scoring + affichage de la carte

st.button(
    "Afficher la carte" if st.session_state["processed_gdf"] is None else "Metter à jour la carte",
    on_click=load_results, kwargs={'df':odis, 'scores_cat':scores_cat})
    # print(strftime("%Y-%m-%d %H:%M:%S", gmtime())+' <---------------------> CLICK <--------------------->')
    # prefs={}
    # prefs, st.session_state["processed_gdf"], st.session_state["selected_geo"] = load_results(df=odis, scores_cat=scores_cat)
print(strftime("%Y-%m-%d %H:%M:%S", gmtime())+' Ready')



# Main 2 sections with results and map
st.write(st.session_state['processed_gdf'] is not None)
if (st.session_state['processed_gdf'] is not None):
    col_results, col_map = st.columns([1, 2])
    
    ### Results Column
    with col_results:
        st.header("Meilleurs résultats")
        st.markdown('<style>di.st-key-button_top1 .stBUtton button div p  {text-align: left !important;color: red;}</style>',unsafe_allow_html=True)
        if st.button("Afficher tous les meilleurs résultats sur la carte", type='tertiary'):
                st.session_state["fg_list"] = 'All'
        if st.session_state["processed_gdf"] is not None:
            fg_dict_key = build_top_results(st.session_state["processed_gdf"], st.session_state['prefs'])


    ### Map Column
    with col_map:
        st.header("Carte Interactive")
        # we have scoring results let's draw the the map
        fg_list = []
        fg_dict = {}
        #scoring results map with shades + highlighted top 5 binomes 
        fg_dict, fg_list = show_all_results(st.session_state['processed_gdf'][['codgeo','polygon','polygon_binome','weighted_score']], fg_dict, fg_list, st.session_state['prefs'])
        #We finally add all the created feature_groups to the 'All' option
        print('done')
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
        
        # We store the dict for reruns
        st.session_state["fg_dict"] = fg_dict
        st.session_state["fg_list"] = fg_list
    
        # We add additional informational layers
        # We keep the schools in the same academy only
        # See doc here: https://python-visualization.github.io/folium/latest/user_guide/geojson/geojson_marker.html
        # if nb_enfants > 0:
            # fg_dict = show_ecoles()

        print(strftime("%Y-%m-%d %H:%M:%S", gmtime())+' Start Map')
        m = odis_base_map(st.session_state['selected_geo'], st.session_state['prefs'])
        fg = st.radio("Layers", fg_dict.keys())
        st_data = st_folium(
            m,
            feature_group_to_add=fg_dict[st.session_state['fg_dict_key']], #st.session_state["fg_dict"][st.session_state["fg_list"]],
            width=1000,
            height=800,
            key="odis_scored_map",
            use_container_width=True,
            # layer_control=flm.LayerControl(collapsed=False)
        )
        
        # st_folium(
        #     base_map,
        #     feature_group_to_add=fg_results,#fg_dict[fg],
        #     width=400,
        #     height=300,
        #     key="odis_scored_map",
        #     # layer_control=flm.LayerControl(collapsed=False)
        # )
        print(strftime("%Y-%m-%d %H:%M:%S", gmtime())+' End Map')

        