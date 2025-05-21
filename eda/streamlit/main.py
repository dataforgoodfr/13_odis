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
#Default for list items
liste_metiers_adult1=[]
liste_metiers_adult2=[]
liste_formations_adult1=[]
liste_formations_adult2=[]
st.session_state['show_adult2_metiers'] = False
st.session_state['show_adult2_formations'] = False


# Subject preferences weighted score computation
#with st.sidebar.form("preferences", border=False):
penalite = 0.1

#Mobilité
departements = sorted(set(odis['dep_code']))
departement_actuel = st.sidebar.selectbox("Département", departements, index=departements.index('33'))
communes = sorted(set(odis[odis.dep_code==departement_actuel]['libgeo']))
commune_actuelle = st.sidebar.selectbox("Commune", communes, index=communes.index('Bordeaux'))
commune_codgeo = odis[(odis.dep_code==departement_actuel) & (odis.libgeo==commune_actuelle)].codgeo.item()
loc_distance_km = st.sidebar.select_slider("Distance Max Relocalisation", range(0,51), value=5)

#Metiers
libfap = sorted(set(codfap_index['Intitulé FAP 341']))
libform = sorted(set(codformations_index['libformation']))
liste_metiers_adult1 = st.sidebar.multiselect("Métiers ciblés P1",libfap)
st.session_state['show_adult2_metiers'] = st.sidebar.checkbox('Ajouter Adulte',value=False, key='ajout_adult_metiers')
if st.session_state['show_adult2_metiers']:
    liste_metiers_adult2 = st.sidebar.multiselect("Métiers ciblés P2",libfap)
#Formations


liste_formations_adult1 = st.sidebar.multiselect("Formations ciblées P1",libform)
st.session_state['show_adult2_formations'] = st.sidebar.checkbox('Ajouter Adulte', key='ajout_adult_formations')
if st.session_state['show_adult2_formations']:
    liste_formations_adult2 = st.sidebar.multiselect("Formations ciblées P2",libform)

#Poids
poids_emploi = st.sidebar.select_slider("Pondération Emploi", range(0,5), value=1)
poids_logement = st.sidebar.select_slider("Pondération Logement", range(0,5), value=1)
poids_education = st.sidebar.select_slider("Pondération Education", range(0,5), value=1)
poids_soutien = st.sidebar.select_slider("Pondération Soutien", range(0,5), value=1)
poids_mobilité = st.sidebar.select_slider("Pondération Mobilité", range(0,5), value=1)

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
        'codes_metiers_adulte1':liste_metiers_adult1,
        'codes_metiers_adulte2':liste_metiers_adult2
    },
    'codes_formations':{
        'codes_formations_adulte1':liste_formations_adult1,
        'codes_formations_adulte2':liste_formations_adult2
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

COM_LIMITROPHE_PENALTY = 0.8 #0.1 = décote de 10% pour les communes limitrophes vs commune cible 

if "data_processed" not in st.session_state:
    st.session_state["data_processed"] = False
if "processed_gdf" not in st.session_state:
    st.session_state["processed_gdf"] = None
if "folium_map_object" not in st.session_state:
    st.session_state["folium_map_object"] = None
if "selected_geo" not in st.session_state:
    st.session_state["selected_geo"] = None

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

if submitted: #st.button("Afficher la carte"):
    st.session_state["data_processed"] = False
    st.session_state["folium_map_object"] = None
    st.session_state["selected_geo"] = None
    st.session_state["selected_binome"] = None
    # Computing Weighted Scores given a source dataframe with geo info and a scoring categorisation
    st.session_state["processed_gdf"] = compute_odis_score(odis, scores_cat=scores_cat, prefs=prefs)




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