#TODO
# Add Rank on map
# Auto Zoom
# Show Schools

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
from scipy import stats
import shapely as shp
from shapely.wkt import loads
from shapely.geometry import Polygon
from sklearn import preprocessing

# Streamlit App
import streamlit as st
import streamlit.components.v1 as components
import folium as flm
from streamlit_folium import folium_static, st_folium
from branca.colormap import linear
from odisscoring import compute_odis_score, produce_pitch

st.set_page_config(layout="wide", page_title='Odis Stream2 Prototype')

# Executing the original Notebook with all the functions and ground work to get the Odis Dataframe


# setting up all the session_states
# Fetched Datasets
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
if "context_set" not in st.session_state:
    st.session_state["context_set"] = False
if "prefs_set" not in st.session_state:
    st.session_state["prefs_set"] = False
if "prefs" not in st.session_state:
    st.session_state["prefs"] = None
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
if 'zoom' not in st.session_state:
    st.session_state["zoom"] = None

st.sidebar.write("Odis Stream #2 Prototype App")

def load_results(): 
    # We reset some states
    st.session_state['prefs'] = prefs
    st.session_state["selected_geo"] = None
    st.session_state["selected_binome"] = None
    st.session_state["fg_list"] = "Scores+top1"
    # Computing Weighted Scores given a source dataframe with geo info and a scoring categorisation
    st.toast('Scoring, please wait...')
    st.session_state["processed_gdf"] = compute_odis_score(odis, scores_cat, prefs).sort_values('weighted_score', ascending=False).reset_index(drop=True)
    st.session_state["processed_gdf"].to_crs(epsg=4326, inplace=True)
    st.session_state["selected_geo"] = st.session_state["processed_gdf"][st.session_state["processed_gdf"].codgeo == st.session_state['prefs']['commune_actuelle']]


#Avoid reloding the notebook all the time
if st.session_state["odis"] is None:
    exec(open("./jacques-scoring.py").read()) 
    st.session_state["odis"] = odis
    st.session_state["codfap_index"] = codfap_index
    st.session_state["codformations_index"] = codformations_index
    st.session_state["annuaire_ecoles"] = annuaire_ecoles
    st.session_state["scores_cat"] = scores_cat
    st.toast("Dateset loaded...")
else:
    odis = st.session_state["odis"]
    codfap_index = st.session_state["codfap_index"]
    codformations_index = st.session_state["codformations_index"]
    annuaire_ecoles = st.session_state["annuaire_ecoles"]
    scores_cat = st.session_state["scores_cat"]

# Subject preferences weighted score computation
#with st.sidebar.form("preferences", border=False):

# Sidebar
with st.sidebar:
    #Mobilité
    departements = sorted(set(odis['dep_code']))
    departement_actuel = st.selectbox("Département", departements, index=departements.index('33'))
    communes = sorted(set(odis[odis.dep_code==departement_actuel]['libgeo']))
    commune_actuelle = st.selectbox("Commune", communes)
    commune_codgeo = odis[(odis.dep_code==departement_actuel) & (odis.libgeo==commune_actuelle)].codgeo.item()
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
    col_emploi, col_formation, col_logement, col_edu, col_sante = st.columns(5)

    #Metiers
    with col_emploi:
        with st.popover("Emploi", use_container_width=True):
            #Default for list items
            liste_metiers_adult=[[],[]]
            libfap = sorted(set(codfap_index['Intitulé FAP 341']))
            libform = sorted(set(codformations_index['libformation']))
            for adult in range(0,nb_adultes):
                liste_metiers_adult[adult] = st.multiselect("Métiers ciblés Adulte "+str(adult+1),libfap)

    #Formations
    with col_formation:
        with st.popover("Formations", use_container_width=True):
            liste_formations_adult=[[],[]]
            for adult in range(0,nb_adultes):
                liste_formations_adult[adult] = st.multiselect("Formations ciblées Adulte "+str(adult+1),libform)

    # Logement
    with col_logement:
        with st.popover("Logement", use_container_width=True):
            st.radio('Quel logement à court terme',["Chez l'habitant", 'Location', "Logement Social"])
            st.radio('Quel logement à long terme',['Location', "Logement Social"])

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

    #Bouton Pour lancer le scoring + affichage de la carte
    # Bouton Afficher la Carte
    if st.session_state["processed_gdf"] is None:
        st.button("Afficher la carte", on_click=load_results)
    else:
        st.button("Mettre à jour la carte", on_click=load_results)

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
        'age_enfant1':4,
        'age_enfant2':10,
        'age_enfant3':None,
        'age_enfant4':None,
        'age_enfant5':None
    },
    'binome_penalty':penalite_binome
}


#First let set the styles of each type of items 
def styling_communes(feature, style):
    score_weights_dict = st.session_state["processed_gdf"].set_index("codgeo")["weighted_score"]
    colormap = linear.YlGn_09.scale(st.session_state["processed_gdf"]['weighted_score'].min(), st.session_state["processed_gdf"]['weighted_score'].max())
    
    if style == 'style_score':
        style_to_use = {
            #"fillColor": "#1100f8",
            "fillColor": colormap(score_weights_dict[feature["properties"]['codgeo']]),
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

def add_commune_to_map(df, geometry, fg, style, index):
    commune_json = flm.GeoJson(data=df, style_function=lambda feature: styling_communes(feature=feature, style=style))#, icon=flm.features.DivIcon(html='<div style="font-size: 24pt">%s</div>' % 'toto'))
    fg.add_child(commune_json)
    #fg.add_child(flm.Tooltip(str(index+1)))

def isolate_commune_on_map(index):
    st.session_state["fg_list"]="Scores+top"+str(index+1)

# Main 2 sections with results and map
if st.session_state['processed_gdf'] is not None:
    col_results, col_map = st.columns([1, 2])
    with col_results:
        st.header("Meilleurs résultats")
        if st.button("Afficher tous les meilleurs résultats sur la carte", type='tertiary'):
                st.session_state["fg_list"] = 'All'
        if st.session_state["processed_gdf"] is not None:
            show_on_map_buttons={}
            for index, row in st.session_state["processed_gdf"].head(5).iterrows():
                if row.binome:
                    title = "Top " + str(index+1) + ': '+row.libgeo+' (en binôme avec '+row.libgeo_binome+')'
                else:
                    title = "Top " + str(index+1) + ': '+row.libgeo+' (seule)'
                if st.button(title, use_container_width=True, on_click=isolate_commune_on_map, kwargs={'index':index}):
                    with st.container(border=True):
                        pitch=produce_pitch(row, st.session_state['scores_cat'])
                        for row in pitch:
                            st.write(row) 
                # expanded_state = bool(index==0)
                # with st.expander(title, expanded=expanded_state):
                #     st.button("Show on map", key=title, type='secondary', on_click=isolate_commune_on_map, kwargs={'index':index})
                #     pitch=produce_pitch(row)
                #     for row in pitch:
                #         st.write(row)      
            
            

    with col_map:
        st.header("Carte Interactive")
        # we have scoring results let's draw the the map
        #base map
        st.session_state["center"] = [
            st.session_state["selected_geo"].centroid.y.iloc[0],
            st.session_state["selected_geo"].centroid.x.iloc[0]
            ]
        st.session_state["zoom"] = 10
        m = flm.Map(location=st.session_state["center"], zoom_start=st.session_state["zoom"])
        
        
        fg_list = []
        fg_dict = {}
        
        #scoring results map
        fg_results = flm.FeatureGroup(name="Results")
        fg_list += [fg_results]
        fg_dict['Scores']=fg_results
        for index, row in st.session_state["processed_gdf"].iterrows():
            df = st.session_state["processed_gdf"][st.session_state["processed_gdf"].index==index][['codgeo','polygon', 'weighted_score']]
            add_commune_to_map(df=df, geometry='polygon', fg=fg_results, style='style_score', index=index)
            
        #top N communes maps
        for index, row in st.session_state["processed_gdf"].head(5).iterrows():
            fg_name = 'fg_com'+str(index+1)
            fg_name = flm.FeatureGroup(name="Commune Top"+str(index+1))
            fg_list += [fg_name]
            fg_name_dict="Scores+top"+str(index+1)
            fg_dict[fg_name_dict]=[fg_results,fg_name]
            df = st.session_state["processed_gdf"][st.session_state["processed_gdf"].index==index][['codgeo','polygon']]
            add_commune_to_map(df=df, geometry='polygon', fg=fg_name, style='style_target', index=index)
            #Same thing for corresponding binomes
            df_binome = st.session_state["processed_gdf"][st.session_state["processed_gdf"].index==index][['codgeo','polygon_binome']]
            df_binome = gpd.GeoDataFrame(df_binome, geometry='polygon_binome', crs='EPSG:2154').to_crs(epsg=4326)
            add_commune_to_map(df=df_binome, geometry='polygon_binome', fg=fg_name, style='style_binome', index=index)

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
        # We store the selected feature group to a session_state so that on reload it picks the right options
        fg=st.session_state["fg_list"]
    
        #We add additional informational layers
        # We keep the schools in the same academy only
        # See doc here: https://python-visualization.github.io/folium/latest/user_guide/geojson/geojson_marker.html
        if nb_enfants > 0:
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
                fg_dict[fg].append(fg_ecoles)
                st.write(':large_blue_circle:= Ecoles,:red_circle:= Collèges et :large_green_circle:= Lycées')
            elif fg_ecoles in fg_dict[fg]:
                fg_dict[fg].remove(fg_ecoles)
            
        
        #Finally we display the map with all feature groups
        st_folium(
            m,
            center=st.session_state["center"],
            zoom=st.session_state["zoom"],
            feature_group_to_add=fg_dict[fg],
            width=1000,
            height=800,
            key="odis_scored_map",
            # layer_control=flm.LayerControl(collapsed=False)
        )   
        
        
        