#TEST GITHUB FROM CHROMEBOOK


# Streamlit App
import streamlit as st
import streamlit.components.v1 as components
import folium as flm
from streamlit_folium import folium_static, st_folium
from branca.colormap import linear

# Executing the original Notebook with all the functions and ground work to get the Odis Dataframe
#st.toast("loading dataset...")
exec(open("./jacques-scoring.py").read())
 

st.set_page_config(layout="wide", page_title='Odis Stream2 Prototype')
st.sidebar.write("Odis App")

# Subject preferences weighted score computation
#with st.sidebar.form("preferences", border=False):

# Sidebar
with st.sidebar:
    #Mobilité
    departements = sorted(set(odis['dep_code']))
    departement_actuel = st.sidebar.selectbox("Département", departements, index=departements.index('33'))
    communes = sorted(set(odis[odis.dep_code==departement_actuel]['libgeo']))
    commune_actuelle = st.sidebar.selectbox("Commune", communes, index=communes.index('Bordeaux'))
    commune_codgeo = odis[(odis.dep_code==departement_actuel) & (odis.libgeo==commune_actuelle)].codgeo.item()
    loc_distance_km = st.sidebar.select_slider("Distance Max Relocalisation", range(0,51), value=5)

    #Foyer
    nb_adultes = st.sidebar.select_slider("Nombre d'adultes", range(1,3), value=1)
    nb_enfants = st.sidebar.select_slider("Nombre d'enfants", range(0,6), value=1)

    #Poids
    with st.sidebar.expander("Pondérations", expanded=False):
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
submitted = st.button("Afficher la carte")

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

if "data_processed" not in st.session_state:
    st.session_state["data_processed"] = False
if "processed_gdf" not in st.session_state:
    st.session_state["processed_gdf"] = None
if "folium_map_object" not in st.session_state:
    st.session_state["folium_map_object"] = None
if "selected_geo" not in st.session_state:
    st.session_state["selected_geo"] = None
if "afficher_commune" not in st.session_state:
    st.session_state["afficher_commune"] = False
if "commune_json" not in st.session_state:
    st.session_state["commune_json"] = None

if submitted: #st.button("Afficher la carte"):
    st.session_state["data_processed"] = False
    st.session_state["folium_map_object"] = None
    st.session_state["selected_geo"] = None
    st.session_state["selected_binome"] = None
    st.session_state['afficher_commune'] = False
    st.session_state['commune_json'] = None
    
    # Computing Weighted Scores given a source dataframe with geo info and a scoring categorisation
    st.session_state["processed_gdf"] = compute_odis_score(odis, scores_cat=scores_cat, prefs=prefs).sort_values('weighted_score', ascending=False).reset_index(drop=True)

def add_commune_to_map(df, geometry, fg, style):
    # commune = gpd.GeoDataFrame(df, geometry=geometry).set_crs(epsg=4326)
    commune_json = flm.GeoJson(data=df, style_function=lambda _x: style)
    fg.add_child(commune_json)

def draw_folium_map(df, fg):
    df.to_crs(epsg=4326, inplace=True) #4326
    score_weights_dict = df.set_index("codgeo")["weighted_score"]
    colormap = linear.YlGn_09.scale(
        df['weighted_score'].min(), df['weighted_score'].max()
    )
    m = flm.Map(location=[lat,lon], zoom_start=10)
    communes = gpd.GeoDataFrame(df[['codgeo','polygon','weighted_score']])
    communes_json = flm.GeoJson(data=communes, style_function=lambda feature: {"stroke":False, "fillOpacity":0.8, "fillColor": colormap(score_weights_dict[feature["properties"]['codgeo']])})
    fg.add_child(communes)
    #communes_json.add_to(m)

# Main 2 sections with results and map
col_results, col_map = st.columns(2)
with col_results:
    st.header("Meilleurs résultats")
    if st.session_state["processed_gdf"] is not None:
        for index, row in st.session_state["processed_gdf"].head(5).iterrows():
            if row.binome:
                title = "Top " + str(index+1) + ': '+row.libgeo+' (en binôme avec '+row.libgeo_binome+')'
            else:
                title = "Top " + str(index+1) + ': '+row.libgeo+' (seule)'
            with st.expander(title, expanded=False):
                #st.button("Afficher Top"+str(index+1),on_click=add_commune_on_map, kwargs={'row_index':index}, type='tertiary')
                pitch=produce_pitch(row)
                for row in pitch:
                    st.write(row)
        

with col_map:
    st.header("Carte Interactive")

    if st.session_state["processed_gdf"] is not None:
        # we have scoring results let's draw the the map
        
        style_score = {
            "fillColor": "#1100f8",
            "color": "#1100f8",
            "fillOpacity": 0.13,
            "weight": 2,
        }
        style_target = {
            "color": "#ff3939",
            "fillOpacity": 0,
            "weight": 3,
            "opacity": 1,
        }

        style_binome = {
            "color": "#ff3939",
            "fillOpacity": 0,
            "weight": 3,
            "opacity": 0.5,
            "dashArray": "5, 5",
        }
        
        #base map
        st.session_state["processed_gdf"].to_crs(epsg=4326, inplace=True)
        lat = st.session_state["processed_gdf"][st.session_state["processed_gdf"].codgeo == commune_codgeo].centroid.y
        lon = st.session_state["processed_gdf"][st.session_state["processed_gdf"].codgeo == commune_codgeo].centroid.x
        m = flm.Map(location=[lat,lon], zoom_start=10)
        fg_list = []
        
        #scoring results map
        fg_results = flm.FeatureGroup(name="Results")
        fg_list += [fg_results]
        for index, row in st.session_state["processed_gdf"].head(10).iterrows():
            df = st.session_state["processed_gdf"][st.session_state["processed_gdf"].index==index][['libgeo','polygon']]
            add_commune_to_map(df=df, geometry='polygon', fg=fg_results, style=style_score)
            
        #top N communes maps
        for index, row in st.session_state["processed_gdf"].head(5).iterrows():
            fg_name = 'fg_com'+str(index+1)
            fg_name = flm.FeatureGroup(name="fg_name"+str(index+1))
            fg_list += [fg_name]
            df = st.session_state["processed_gdf"][st.session_state["processed_gdf"].index==index][['codgeo','polygon']]
            add_commune_to_map(df=df, geometry='polygon', fg=fg_name, style=style_target)
            #Same thing for corresponding binomes
            df_binome = st.session_state["processed_gdf"][st.session_state["processed_gdf"].index==index][['codgeo','polygon_binome']]
            df_binome = gpd.GeoDataFrame(df_binome, geometry='polygon_binome', crs='EPSG:2154').to_crs(epsg=4326)
            add_commune_to_map(df=df_binome, geometry='polygon_binome', fg=fg_name, style=style_binome)


        
        #Finally we display the map with all feature groups
        layer = flm.LayerControl(collapsed=False)
        st_data=st_folium(
            m,
            feature_group_to_add=fg_list,
            width=700,
            height=500,
            key="odis_scored_map",
            #layer_control=layer
            )   

    
# folium.Choropleth(
#     geo_data=state_geo,
#     name="choropleth",
#     data=state_data,
#     columns=["State", "Unemployment"],
#     key_on="feature.id",
#     fill_color="YlGn",
#     fill_opacity=0.7,
#     line_opacity=0.2,
#     legend_name="Unemployment Rate (%)",
# ).add_to(m)

        # # st.session_state["processed_gdf"].polygon_binome = st.session_state["processed_gdf"].polygon_binome.apply(shp.from_wkb) 
        # binome = gpd.GeoDataFrame(
        #     st.session_state["processed_gdf"][st.session_state["processed_gdf"].codgeo =='33234'][['codgeo','polygon_binome']],
        #     geometry='polygon_binome', crs='EPSG:4326')
        # # binome = binome.set_geometry('polygon_binome', crs='EPSG:4326')
        # binome = binome.to_crs(epsg=4326)
        # st.write(binome)
        # binome_json = flm.GeoJson(data=binome, style_function=lambda x: {"stroke":True, 'fillColor':'red', 'fillOpacity':1.0, "color":"red"})
        # fg_binome.add_child(binome_json)

        # st_data=st_folium(
        #     m,
        #     feature_group_to_add=fg_dict[fg],#[fg_results, fg_binome],
        #     width=700,
        #     height=500,
        #     key="odis_scored_map",
        #     layer_control=flm.LayerControl(collapsed=False)
        #     )
        # if st_data['last_object_clicked'] is not None:
        #     clicked_point = shp.Point(st_data['last_object_clicked']['lng'], st_data['last_object_clicked']['lat'])
        #     st.session_state["selected_geo"] = (st.session_state['processed_gdf'])[st.session_state["processed_gdf"].contains(clicked_point)]
        #     st.session_state["selected_binome"] = st.session_state["selected_geo"][['codgeo_binome','libgeo_binome','polygon_binome']]
        #     st.session_state["selected_binome"].polygon_binome = st.session_state["selected_binome"].polygon_binome.apply(loads)
        #     st.write(st.session_state["selected_binome"])

        #     # binome = gpd.GeoDataFrame(st.session_state["selected_binome"])
        #     # st.write(binome)
        #     # binome = binome.set_geometry('polygon_binome', crs='EPSG:4326')
        #     # binome = binome.to_crs(epsg=4326)
        #     # binome_json = flm.GeoJson(data=binome, style_function=lambda x: {"stroke":True,'fillColor':'red', 'fillOpacity':1, "color":"red"})
        #     # st.write(binome_json)
        #     # fg_binome.add_child(binome_json)

                
        #     with st.popover('Details'):
        #         pitch = st.session_state["selected_geo"].apply(produce_pitch, axis=1).iloc[0]
        #         for row in pitch:
        #             st.write(row)