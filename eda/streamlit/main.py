#TEST GITHUB FROM CHROMEBOOK


# Streamlit App
import streamlit as st
import streamlit.components.v1 as components
from streamlit_folium import folium_static, st_folium

# Executing the original Notebook with all the functions and ground work to get the Odis Dataframe
#st.toast("loading dataset...")
exec(open("../notebooks/jacques-scoring.py").read())

# Sidebar  
#st.sidebar.write("Odis App")

# Subject preferences weighted score computation
with st.sidebar.form("preferences", border=False):
    penalite = range(0,100)
    libfap = sorted(set(codfap_index['Intitulé FAP 341']))
    departements = sorted(set(odis['dep_code']))
    departement_actuel = st.selectbox("Département", departements, index=departements.index('33'))
    communes = sorted(set(odis[odis.dep_code==departement_actuel]['libgeo']))
    commune_actuelle = st.selectbox("Commune", communes, index=communes.index('Bordeaux'))
    commune_codgeo = odis[(odis.dep_code==departement_actuel) & (odis.libgeo==commune_actuelle)].codgeo.item()
    loc_distance_km = st.select_slider("Distance Max Relocalisation", range(0,51), value=5)
    liste_metiers = st.multiselect("Métiers ciblés",libfap)
    poids_emploi = st.select_slider("Pondération Emploi", range(0,5), value=1)
    poids_logement = st.select_slider("Pondération Logement", range(0,5), value=1)
    poids_education = st.select_slider("Pondération Education", range(0,5), value=1)
    poids_soutien = st.select_slider("Pondération Soutien", range(0,5), value=1)
    poids_mobilité = st.select_slider("Pondération Mobilité", range(0,5), value=1)
    submitted = st.form_submit_button("Afficher la carte")

prefs = [
    ['emploi',poids_emploi],
    ['logement',poids_logement],
    ['education',poids_education],
    ['soutien',poids_soutien],
    ['mobilité',poids_mobilité],
    ['commune_actuelle',commune_codgeo],
    ['loc_distance_km',loc_distance_km],
    ['code_metiers',liste_metiers]
]

COM_LIMITROPHE_PENALTY = 0.8 #0.1 = décote de 10% pour les communes limitrophes vs commune cible 

if "data_processed" not in st.session_state:
    st.session_state["data_processed"] = False
if "processed_gdf" not in st.session_state:
    st.session_state["processed_gdf"] = None
if "folium_map_object" not in st.session_state:
    st.session_state["folium_map_object"] = None
if "selected_geo" not in st.session_state:
    st.session_state["selected_geo"] = None


if submitted: #st.button("Afficher la carte"):
    st.session_state["data_processed"] = False
    st.session_state["folium_map_object"] = None
    st.session_state["selected_geo"] = None
    # Computing Weighted Scores given a source dataframe with geo info and a scoring categorisation
    st.session_state["processed_gdf"] = compute_odis_score(odis, scores_cat=scores_cat, prefs=prefs)


if st.session_state["processed_gdf"] is not None:
    st.session_state["processed_gdf"].to_crs(epsg=4326, inplace=True)
    m = st.session_state["processed_gdf"][['polygon','libgeo','weighted_score']].explore('weighted_score')
    #fg = flm.FeatureGroup(name="Binome")
    #for poly in st.session_state["processed_gdf"]:
    #    fg.add_child(flm.Map(poly))
    st_data=st_folium(
        #st.session_state["processed_gdf"][st.session_state["processed_gdf"].codgeo == commune_codgeo],
        m,
        #feature_group_to_add=fg,
        width=700,
        height=500,
        key="display_processed_map")
    if st_data['last_object_clicked'] is not None:
        clicked_point = shp.Point(st_data['last_object_clicked']['lng'], st_data['last_object_clicked']['lat'])
        #st.write(clicked_point)
        st.session_state["selected_geo"] = (st.session_state['processed_gdf'])[st.session_state["processed_gdf"].contains(clicked_point)]
        st.session_state["selected_binome"] = st.session_state["selected_geo"][['codgeo_binome','libgeo_binome','polygon_binome']]
        #flm.GeoJson(st.session_state["selected_geo"]['polygon_binome'])). add_to(m)
        st.write(st.session_state["selected_binome"])
        with st.popover('Details'):
            pitch = st.session_state["selected_geo"].apply(produce_pitch, axis=1).iloc[0]
            for row in pitch:
                st.write(row)

