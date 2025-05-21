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

# Sidebar  
#st.sidebar.write("Odis App")
st.set_page_config(layout="wide", page_title='Odis Stream2 Prototype')

# Subject preferences weighted score computation
#with st.sidebar.form("preferences", border=False):
penalite = 0.1

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
    with st.sidebar.expander("Weighths", expanded=False):
        poids_emploi = st.select_slider("Pondération Emploi", range(0,5), value=1)
        poids_logement = st.select_slider("Pondération Logement", range(0,5), value=1)
        poids_education = st.select_slider("Pondération Education", range(0,5), value=1)
        poids_soutien = st.select_slider("Pondération Soutien", range(0,5), value=1)
        poids_mobilité = st.select_slider("Pondération Mobilité", range(0,5), value=1)

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
    'binome_penalty':penalite
}

if submitted: #st.button("Afficher la carte"):
    st.session_state["data_processed"] = False
    st.session_state["folium_map_object"] = None
    st.session_state["selected_geo"] = None
    st.session_state["selected_binome"] = None
    # Computing Weighted Scores given a source dataframe with geo info and a scoring categorisation
    st.session_state["processed_gdf"] = compute_odis_score(odis, scores_cat=scores_cat, prefs=prefs)

# Main 2 sections with results and map
col_results, col_map = st.columns(2)
with col_results:
    st.header("Meilleurs résultats")
    if "data_processed" not in st.session_state:
        st.session_state["data_processed"] = False
    if "processed_gdf" not in st.session_state:
        st.session_state["processed_gdf"] = None
    if "folium_map_object" not in st.session_state:
        st.session_state["folium_map_object"] = None
    if "selected_geo" not in st.session_state:
        st.session_state["selected_geo"] = None
    
    if st.session_state["processed_gdf"] is not None:
        for index, row in st.session_state["processed_gdf"].sort_values('weighted_score', ascending=False).head(5).iterrows():
            title = "Top " + str(index+1)
            with st.expander(title, expanded=False):
                st.write(produce_pitch(row))

with col_map:
    st.header("Carte")
    def draw_folium_map(df, current_codgeo):
        df.to_crs(epsg=4326, inplace=True) #4326
        lat = df[df.codgeo == current_codgeo].centroid.y
        lon = df[df.codgeo == current_codgeo].centroid.x
        score_weights_dict = df.set_index("codgeo")["weighted_score"]
        colormap = linear.YlGn_09.scale(
            df['weighted_score'].min(), df['weighted_score'].max()
        )

        m = flm.Map(location=[lat,lon], zoom_start=10)
        communes = gpd.GeoDataFrame(df[['codgeo','polygon','weighted_score']])
        #communes_json = communes.to_json()
        communes_json = flm.GeoJson(data=communes, style_function=lambda feature: {"fillOpacity":0, "fillColor": colormap(score_weights_dict[feature["properties"]['codgeo']])})
        fg_results.add_child(communes_json)
        #communes_json.add_to(m)
        return m

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

    if st.session_state["processed_gdf"] is not None:
        #Let's draw the map
        fg_results = flm.FeatureGroup(name="results")
        fg_binome = flm.FeatureGroup(name="binome")
        st.write(st.session_state["processed_gdf"][st.session_state["processed_gdf"].codgeo=='33281'])

        m = draw_folium_map(st.session_state["processed_gdf"], current_codgeo=commune_codgeo)
        
        # st.session_state["processed_gdf"].polygon_binome = st.session_state["processed_gdf"].polygon_binome.apply(shp.from_wkb) 
        binome = gpd.GeoDataFrame(
            st.session_state["processed_gdf"][st.session_state["processed_gdf"].codgeo =='33234'][['codgeo','polygon_binome']],
            geometry='polygon_binome', crs='EPSG:4326')
        # binome = binome.set_geometry('polygon_binome', crs='EPSG:4326')
        binome = binome.to_crs(epsg=4326)
        st.write(binome)
        binome_json = flm.GeoJson(data=binome, style_function=lambda x: {"stroke":True, 'fillColor':'red', 'fillOpacity':1.0, "color":"red"})
        fg_binome.add_child(binome_json)

        st_data=st_folium(
            m,
            feature_group_to_add=[fg_results, fg_binome],
            width=700,
            height=500,
            key="odis_scored_map",
            layer_control=flm.LayerControl(collapsed=False)
            )
        if st_data['last_object_clicked'] is not None:
            clicked_point = shp.Point(st_data['last_object_clicked']['lng'], st_data['last_object_clicked']['lat'])
            st.session_state["selected_geo"] = (st.session_state['processed_gdf'])[st.session_state["processed_gdf"].contains(clicked_point)]
            st.session_state["selected_binome"] = st.session_state["selected_geo"][['codgeo_binome','libgeo_binome','polygon_binome']]
            st.session_state["selected_binome"].polygon_binome = st.session_state["selected_binome"].polygon_binome.apply(loads)
            st.write(st.session_state["selected_binome"])

            # binome = gpd.GeoDataFrame(st.session_state["selected_binome"])
            # st.write(binome)
            # binome = binome.set_geometry('polygon_binome', crs='EPSG:4326')
            # binome = binome.to_crs(epsg=4326)
            # binome_json = flm.GeoJson(data=binome, style_function=lambda x: {"stroke":True,'fillColor':'red', 'fillOpacity':1, "color":"red"})
            # st.write(binome_json)
            # fg_binome.add_child(binome_json)

                
            with st.popover('Details'):
                pitch = st.session_state["selected_geo"].apply(produce_pitch, axis=1).iloc[0]
                for row in pitch:
                    st.write(row)



# odis_search_best.to_crs(epsg=4326, inplace=True)
# lat = odis_search_best[odis_search_best.codgeo == prefs['commune_actuelle']].centroid.y
# lon = odis_search_best[odis_search_best.codgeo == prefs['commune_actuelle']].centroid.x
# m = flm.Map(location=[lat,lon], zoom_start=10)

# def add_polygons_to_map(df, polygon_column):
#     geo_j = df.loc[polygon_column].__geo_interface__ 
#     geo_j = flm.GeoJson(data=geo_j, style_function=lambda x: {"fillColor": "orange"})
#     geo_j.add_to(m)

# odis_search_best.apply(add_polygons_to_map, polygon_column='polygon', axis=1)
# m