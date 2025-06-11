from time import time
print('############################################')

def performance_tracker(t, text, timer_mode):
    if timer_mode:
        print(str(round(time()-t,2))+'|'+text)
        return time()
t = time()
print(t)
timer_mode = True

t = performance_tracker(t, 'Start Import', timer_mode)
# Notebook Specific
import pandas as pd
# import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
# from scipy import stats
# import shapely as shp
# from shapely.wkt import loads
from shapely.geometry import mapping#, Polygon 
# from sklearn import preprocessing

# Streamlit App Specific
import streamlit as st
import folium as flm
# from folium.plugins import MarkerCluster
from streamlit_folium import st_folium
from branca.colormap import linear
from odis_stream2_scoring import compute_odis_score, init_loading_datasets#, produce_pitch_markdown 

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
    if 'afficher_sante' not in st.session_state:
        st.session_state['afficher_sante'] = None
    if 'fg_extras_dict' not in st.session_state:
        st.session_state["fg_extras_dict"] = {}
    if 'fg_dict_ref' not in st.session_state:
        st.session_state["fg_dict_ref"] = {}
    if 'fg_dict' not in st.session_state:
        st.session_state["fg_dict"] = {}
    if 'pitch' not in st.session_state:
        st.session_state["pitch"] = []
    if 'prefs' not in st.session_state:
        st.session_state["prefs"] = {}
    if "fg_ecoles" not in st.session_state:
        st.session_state["fg_ecoles"] = None
    if "fg_sante" not in st.session_state:
        st.session_state["fg_sante"] = None
    if "zoom" not in st.session_state:
        st.session_state["zoom"] = 10
        
# This @st.cache_resource dramatically improves performance of the app
@st.cache_resource
def init_datasets():
    # We load all the datasets
    odis, scores_cat, codfap_index, codformations_index, annuaire_ecoles, annuaire_sante = init_loading_datasets(ODIS_FILE, SCORES_CAT_FILE, METIERS_FILE, FORMATIONS_FILE, ECOLES_FILE, MATERNITE_FILE, SANTE_FILE)
    
    coddep_set = sorted(set(odis['dep_code']))
    depcom_df = odis[['dep_code','libgeo']].sort_values('libgeo')
    codgeo_df = odis[['dep_code','libgeo']]
    libfap_set = sorted(set(codfap_index['Intitulé FAP 341']))
    libform_set = sorted(set(codformations_index['libformation']))
    
    return odis, codfap_index, codformations_index, annuaire_ecoles, annuaire_sante, scores_cat, coddep_set, depcom_df, codgeo_df, libfap_set, libform_set

@st.cache_data
def compute_score(_df, scores_cat, prefs):
    return compute_odis_score(_df, scores_cat, prefs)

def set_prefs(scores_cat):

    prefs = {
        'poids_emploi':poids_emploi,
        'poids_logement':poids_logement,
        'poids_education':poids_education,
        'poids_soutien':poids_soutien,
        'poids_mobilité':poids_mobilité,
        'commune_actuelle':commune_codgeo,
        'loc_distance_km':loc_distance_km,
        'nb_adultes': nb_adultes,
        'nb_enfants': nb_enfants,
        'hebergement':hebergement,
        'logement':logement,
        'codes_metiers': liste_metiers_adult,
        'codes_formations':liste_formations_adult,
        'classe_enfants':liste_classe_enfants,
        'besoin_sante':besoin_sante,
        'binome_penalty':penalite_binome
    }
    # Add binomes scores & weights to scores_cat
    scores_cat_prefs=scores_cat.copy()
    for index, row in scores_cat.iterrows():
        if row.loc['incl_binome']:
            row_to_add = scores_cat.loc[[index]]
            row_to_add['score'] = row_to_add['score'] + '_binome'
            row_to_add['score_name'] = row_to_add['score_name'] + ' (Binôme)'
            row_to_add['incl_binome'] = False
            scores_cat_prefs = pd.concat([scores_cat_prefs, row_to_add])
    scores_cat_prefs['weight'] = scores_cat_prefs['cat'].apply(lambda x: prefs['poids_'+x])
    scores_cat_prefs.set_index('score', inplace=True)
    
    # st.write(prefs)
    # print(prefs)
    return prefs, scores_cat_prefs


def load_results(df, scores_cat): 
    print(' <---------------------> CLICK <--------------------->')
    prefs, scores_cat_prefs = set_prefs(scores_cat) # We update the prefs with the latest inputs
    # Computing Weighted Scores given a source dataframe with geo info and a scoring categorisation
    
    odis_scored = compute_score(
        _df=df,
        scores_cat=scores_cat,
        prefs=prefs
        )
    
    # We pop the selected commune from the results
    selected_geo = odis_scored.loc[prefs['commune_actuelle']]
    odis_scored = odis_scored.drop(prefs['commune_actuelle'])
    # We sort so that we can have the top results by selecting the first rows
    odis_scored = odis_scored.sort_values('weighted_score', ascending=False).reset_index()
    
    # 10,25,50,100,500,1000
    match prefs['loc_distance_km']:
        case 10:
            st.session_state['zoom'] = 11
        case 25:
            st.session_state['zoom'] = 10
        case 50:
            st.session_state['zoom'] = 9
        case 100:
            st.session_state['zoom'] = 8
        case 500:
            st.session_state['zoom'] = 6
        case 100:
            st.session_state['zoom'] = 5

    st.session_state['scores_cat'] = scores_cat_prefs
    st.session_state['processed_gdf'] = odis_scored
    st.session_state['selected_geo'] = selected_geo
    st.session_state['prefs'] = prefs
    st.session_state['afficher_ecoles'] = True
    st.session_state['afficher_sante'] = True

def styling_communes(style, **kwargs):

    if style == 'style_current':
        style_to_use = {
            "fillColor": 'blue',
            "fillOpacity": 0.8,
            "stroke": True
        }
    if style == 'style_score':
        style_to_use = {
            "fillColor": kwargs['colormap'](kwargs['scores_dict'][kwargs['codgeo']]),
            "fillOpacity": 0.8,
            "stroke": False,
        }
    if style == 'style_target':
        style_to_use = {
            "color": "red",
            "fillOpacity": 0,
            "weight": 3,
            "opacity": 1,
            "html":'<div style="width: 10px;height: 10px;border: 1px solid black;border-radius: 5px;background-color: orange;">1</div>'
        }
    if style == 'style_binome':
        style_to_use = {
            "color": "red",
            "fillOpacity": 0,
            "weight": 2,
            "opacity": 1,
            "dashArray": "5, 5",
        }

    return style_to_use

@st.cache_data
def add_commune_to_map(_df, _fg, style, prefs, index):
    style_to_use = styling_communes(feature=_df.codgeo.iloc[0], style=style)
    commune_json = flm.GeoJson(data=_df, style=style_to_use)
    _fg.add_child(commune_json)
    return _fg
    #fg.add_child(flm.Tooltip(str(index+1)))

def show_fg(_fg_dict, fg_key, clear_others):
    # We first remove all the other TopX fgs and then add the one we care about
    if clear_others:
        for fg in [k for k in _fg_dict.keys() if k.startswith('Top')]:
            _fg_dict.pop(fg)
    _fg_dict[fg_key] = st.session_state["fg_dict_ref"][fg_key]
    return _fg_dict

# @st.cache_data
def result_highlight(row, index, prefs):
    # For each of the top N result we
    # Add the red outline to show the commune on the scored communes map
    st.session_state["fg_dict"] = show_fg(_fg_dict=st.session_state["fg_dict"], fg_key='Top'+str(index+1), clear_others=True)
    # We generate the pitch for this commune
    st.session_state['pitch'] = produce_pitch_markdown(row=row, prefs=st.session_state["prefs"], scores_cat=st.session_state['scores_cat'], codfap_index=codfap_index, codformations_index=codformations_index)

def build_top_results(_df, n, prefs):
    cols_to_show = [col for col in _df.columns if ('_ratio' or '_score' or '_ratio') in col]
    
    for index, row in _df.head(n).iterrows():
        # We show the proposed commune with a solid red border and binome with dashed one (if any)
        fg_name = flm.FeatureGroup(name="Commune Top"+str(index+1))
        geojson_features = []

        # Top 5 with individual feature groups for each commune+binome pair
        if row.polygon is not None:
        
            geojson_geometry = mapping(row.polygon)

            style_to_use = styling_communes(style='style_target')
            properties = {
                "Nom": row.libgeo,
                "Score": round(row.weighted_score, 2),
                "style": style_to_use,
                "popup_html": ( # Pre-render popup HTML for convenience
                    f"<b>{row.libgeo}</b><br>"
                    f"Score: {row.weighted_score:.2f}<br>"
                    f"Binôme: {row.libgeo_binome}"
                )
            }

            commune = {
                "type": "Feature",
                "geometry": geojson_geometry,
                "properties": properties
            }

            geojson_features.append(commune)


            # Handle the binome if any
            if row.polygon_binome is not None:
                # This time we display the binome polygon
                geojson_geometry = mapping(row.polygon_binome)

                style_to_use = styling_communes(style='style_binome')
                properties = {
                    "Nom": row.libgeo_binome,
                    "Score": round(row.weighted_score, 2),
                    "style": style_to_use,
                }

                commune = {
                    "type": "Feature",
                    "geometry": geojson_geometry,
                    "properties": properties
                }
                geojson_features.append(commune)

            # Now we add both to a Feature Collection
            geojson_data = {
                "type": "FeatureCollection",
                "features": geojson_features
            }
            commune_json = flm.GeoJson(geojson_data, tooltip=flm.GeoJsonTooltip(fields=['Nom', 'Score'], aliases=['Nom', 'Score']))
            fg_name.add_child(commune_json)
        
        # We register this fg into our refence dict of all possible fgs
        st.session_state['fg_dict_ref']['Top'+str(index+1)] = fg_name


        # Then we can show the expander-like button (= expander with on-click and)
        title = f"Top {index+1}: {row.libgeo} (en binôme avec {row.libgeo_binome})" if row.binome else f"Top {index+1}: {row.libgeo}"
        if st.button(
            title,
            on_click=result_highlight,
            kwargs={'row':row, 'index':index, 'prefs':prefs},
            use_container_width=True,
            key='button_top'+str(index+1),
            type='secondary'
            ):
            with st.container(border=True):
                st.markdown(st.session_state['pitch'])
                # with st.expander('Détails Supplémentaires'):
                #     for col, value in row.filter(items=cols_to_show).items():
                #         st.write(f"{st.session_state['scores_cat'].loc[col].score_name} | Score: {value}")
            
    # This is used as a callback so we don't return anything

def produce_pitch_markdown(row, prefs, scores_cat, codfap_index, codformations_index):
    # We produce the pitch for the top N results
    # We generate a markdown text that is natively displayed by streamlit
    pitch_md = []
    pitch_md.append(f'**{row["libgeo"]}** dans l\'EPCI: {row["epci_nom"]}.  ')
    pitch_md.append(f'Le score est de: **{row["weighted_score"]:.2f}**.')
    if row["binome"]:
        pitch_md.append(f'Ce score est obtenu en binome avec la commune {row["libgeo_binome"]}')
        if row["epci_code"] != row["epci_code_binome"]:
            pitch_md.append(f' située dans l\'EPCI: {row["epci_nom_binome"]}')
    else:
        pitch_md.append(f'Ce score est obtenu sans commune binôme')

    # Adding the top contributing criterias
    crit_scores_col = [col for col in row.keys() if '_scaled' in col]
    
    # First compute the weighted (+ penalty for binome) for each criteria score
    weighted_row=row.copy()
    for col in crit_scores_col:
        weight = scores_cat.loc[col]['weight'].item()
        weighted_row[col] =  row[col] * weight * (1-prefs['binome_penalty']) if col.endswith('_binome') else row[col] * weight
    # Then we sort an print the top 5
    weighted_row = weighted_row[crit_scores_col].dropna().sort_values(ascending=False)

    for i in range(0, 5):
        score = weighted_row.index[i]
        score_name = scores_cat.loc[score]['score_name']
        pitch_md.append(f'- Le critère #{i+1} est: **{score_name}** avec un score de: **{weighted_row.iloc[i]:.2f}**')

    
    #Adding the matching job families if any
    if len(prefs['codes_metiers']) > 0:
        pitch_md.append('\n**Emploi**\n') 
        metiers_col = [col for col in row.keys() if col.startswith('met_match_codes')]
        matched_codfap_names = []
        for metiers_adultx in metiers_col:
            matched_codfap_names += list(codfap_index[codfap_index['Code FAP 341'].isin(row[metiers_adultx])]['Intitulé FAP 341'])
        matched_codfap_names = list(set(matched_codfap_names))
        if len(matched_codfap_names) == 0:
            pitch_md.append(f'Aucun des métiers recherchés ne figure dans le Top 10 des métiers à pourvoir sur cette zone.  ')
            pitch_md.append('\n')
            pitch_md.append(f'Top 10 des métiers à pourvoir sur cette zone:  ')
            pitch_md.append(f'{", ".join(row["be_libfap_top"])}  ')
        if len(matched_codfap_names) == 1:
            pitch_md.append(f' La famille de métiers **{matched_codfap_names[0]}** est rechechée  ')
        elif len(matched_codfap_names) >= 1:
            pitch_md.append(f'- Les familles de métiers **{", ".join(matched_codfap_names)}** sont rechechées  ')
        
    

    # Adding the matching formation families if any
    if len(prefs['codes_formations']) > 0:
        pitch_md.append('\n**Formations**\n') 
        formations_col = [col for col in row.keys() if col.startswith('form_match_codes')]
        matched_codform_names = []
        for formations_adultx in formations_col:
            matched_codform_names += list(codformations_index[codformations_index.index.isin(row[formations_adultx])]['libformation'])
        matched_codform_names = list(set(matched_codform_names))
        if len(matched_codform_names) == 0:
            pitch_md.append(f'Aucune des formations recherchées ne figure dans les formations offertes sur cette zone.  ')
        if len(matched_codform_names) == 1:
            pitch_md.append(f'- La formation recherchée **{matched_codform_names[0]}** est proposée  ')
        elif len(matched_codform_names) >= 1:
            pitch_md.append(f'- Les formations recherchés **{", ".join(matched_codform_names)}** sont proposées  ')

    return "\n".join(pitch_md)

@st.cache_data
def show_scoring_results(_df, _fg_dict, n, prefs):
    st.session_state['fg_dict_ref'] = {}
    st.session_state['fg_dict'] = {}
    
    # We show all the communes in the search radius in a shaded greeen according to their scores
    fg_results = flm.FeatureGroup(name="Results")

    # We pass all the scored communes at once inside a geojson feature group
    geojson_features = []
    score_weights_dict = st.session_state["processed_gdf"].set_index("codgeo")["weighted_score"]
    colormap = linear.YlGn_09.scale(st.session_state["processed_gdf"]['weighted_score'].min(), st.session_state["processed_gdf"]['weighted_score'].max())

    # All results colored based on score
    for row in _df.itertuples(index=False):
       
        if row.polygon is None:
            continue
    
        geojson_geometry = mapping(row.polygon)

        style_to_use = styling_communes(style='style_score', scores_dict=score_weights_dict, colormap=colormap, codgeo=row.codgeo)
        properties = {
            "Nom": row.libgeo,
            "Score": round(row.weighted_score, 2),
            "style": style_to_use,
            "popup_html": ( # Pre-render popup HTML for convenience
                f"<b>{row.libgeo}</b><br>"
                f"Score: {row.weighted_score:.2f}<br>"
                f"Binôme: {row.libgeo_binome}"
            )
        }

        commune = {
            "type": "Feature",
            "geometry": geojson_geometry,
            "properties": properties
        }
        geojson_features.append(commune)

    # We also add the current commune in Blue
    geojson_geometry = mapping(st.session_state['selected_geo'].polygon)
    style_to_use = styling_communes(style='style_current')
    properties = {
        "Nom": st.session_state['selected_geo'].libgeo,
        "style": style_to_use
    }
    current_commune = {
            "type": "Feature",
            "geometry": geojson_geometry,
            "properties": properties
        }
    geojson_features.append(current_commune)
    
    
    
    # Wrap all features in a GeoJSON FeatureCollection
    geojson_data = {
        "type": "FeatureCollection",
        "features": geojson_features
    }

    communes = flm.GeoJson(
        geojson_data,
        name="Communes",
        # style_function=style_function,
        tooltip=flm.GeoJsonTooltip(fields=['Nom', 'Score'], aliases=['Nom', 'Score']),
        popup=flm.GeoJsonPopup(fields=['popup_html'], aliases=['Details'], localize=True, parse_html=True) # Use pre-rendered HTML
    )
    
    _fg_dict['Scores'] = fg_results
    fg_results.add_child(communes)

    return _fg_dict

@st.cache_data
def odis_base_map(_current_geo, prefs):
    # We store the last known prefs in session to avoid reload
    gs = gpd.GeoSeries(_current_geo.polygon, crs='EPSG:2154')
    center_loc = [
            gs.centroid.y.iloc[0],
            gs.centroid.x.iloc[0]
        ]
    m = flm.Map(location=center_loc, zoom_start=10)
    return m

# Ecoles Processing
@st.cache_data
def filter_ecoles(_current_geo, annuaire_ecoles, prefs):
    # we consider all the etablissements soclaires in the target codgeos and the ones around (voisins)
    target_codgeos = set(
        st.session_state['processed_gdf'].codgeo.tolist() 
        +[x for y in st.session_state['processed_gdf'].codgeo_voisins.tolist() for x in y]
        )
    niveaux_enfants = set(liste_classe_enfants)
    if 'Maternelle' in niveaux_enfants:
        maternelle_df = annuaire_ecoles[annuaire_ecoles.ecole_maternelle > 0]
    else:
        maternelle_df = None
    if 'Primaire' in niveaux_enfants:
        primaire_df = annuaire_ecoles[annuaire_ecoles.ecole_elementaire > 0]
    else:
        primaire_df = None
    if 'Collège' in niveaux_enfants:
        college_df = annuaire_ecoles[annuaire_ecoles.type_etablissement == 'Collège']
    else:
        college_df = None
    if 'Lycée' in niveaux_enfants:
        lycee_df = annuaire_ecoles[annuaire_ecoles.type_etablissement == 'Lycée']
    else:
        lycee_df = None    
    annuaire_ecoles = pd.concat([maternelle_df, primaire_df, college_df, lycee_df])
    mask = annuaire_ecoles['code_commune'].isin(target_codgeos)
    filtered_ecoles=annuaire_ecoles[mask]
    return filtered_ecoles

@st.cache_data
def build_local_ecoles_layer(_current_geo, _annuaire_ecoles, prefs):
    category_colors = {
        'Ecole': 'orange',
        'Collège': 'red',
        'Lycée': 'green',
        }
    fg_ecoles = flm.FeatureGroup(name="Établissements Scolaires")
    filtered_ecoles = _annuaire_ecoles[_annuaire_ecoles.type_etablissement.isin(['Ecole', 'Collège', 'Lycée'])]
    filtered_ecoles = filter_ecoles(_current_geo, filtered_ecoles, prefs)
    
    # Now let's add these schools to the map in the fg_ecoles feature group
    filtered_ecoles = gpd.GeoDataFrame(filtered_ecoles, geometry='geometry', crs='EPSG:4326')
    
    geojson_features = []
    for row in filtered_ecoles.itertuples(index=False):
        if row.geometry is None:
            continue
   
        maternelle_presente = 'Oui' if row.ecole_maternelle > 0 else 'Non'
        properties = {
            "name": row.nom_etablissement,
            "type": row.type_etablissement,
            "Maternelle": maternelle_presente,
            "color": 'blue', # Embed color in properties
            "popup_html": f"<b>{row.nom_etablissement}</b><br>Type: {row.type_etablissement}<br>Maternelle: {maternelle_presente}"
        }

        etablissement = {
            "type": "Feature",
            "geometry": mapping(row.geometry), # Convert shapely Point to GeoJSON dict
            "properties": properties
        }

        geojson_features.append(etablissement)
    
    geojson_data = {
        "type": "FeatureCollection",
        "features": geojson_features
    }
    
    
    etablisssement = flm.GeoJson(
        geojson_data,
        marker=flm.Circle(
            radius=200,
            fill_color="purple",
            fill_opacity=1,
            stroke=False)
    )

    fg_ecoles.add_child(etablisssement)
    # print(fg_ecoles)
    return fg_ecoles

@st.cache_data
def filter_sante(target_codgeos, _annuaire_sante, prefs):
    # we consider all the etablissements soclaires in the target codgeos and the ones around (voisins)
    filtered_sante = _annuaire_sante[_annuaire_sante['codgeo'].isin(target_codgeos)]

    if prefs['besoin_sante'] == 'Maternité':
        filtered_sante = filtered_sante[filtered_sante.maternite]
    elif prefs['besoin_sante'] == "Hopital":
        cat_list = ['355', '362', '101', '106']
        filtered_sante = filtered_sante[filtered_sante.Categorie.isin(cat_list)]
    elif prefs['besoin_sante'] == "Centre Addictions & Maladies Mentales":
        cat_list = ['156', '292', '425', '412', '366', '415', '430', '444']
        filtered_sante = filtered_sante[filtered_sante.Categorie.isin(cat_list)]
    
    return filtered_sante[['nofinesset', 'codgeo', 'RaisonSociale', 'LibelleCategorieAgregat', 'LibelleSph', 'geometry', 'maternite']]

@st.cache_data
def build_local_sante_layer(_current_geo, _annuaire_sante, prefs):
    fg_sante = flm.FeatureGroup(name="Établissements de Santé")
    # fg_sante = MarkerCluster(name="Établissements de Santé")

    target_codgeos = set(
        st.session_state['processed_gdf'].codgeo.tolist() 
        +[x for y in st.session_state['processed_gdf'].codgeo_voisins.tolist() for x in y]
        )

    filtered_annuaire = filter_sante(target_codgeos, _annuaire_sante, prefs)
    filtered_annuaire = gpd.GeoDataFrame(filtered_annuaire, geometry='geometry', crs='EPSG:2154')
    filtered_annuaire.to_crs(epsg=4326, inplace=True)
    
    geojson_features = []
    for row in filtered_annuaire.itertuples(index=False):
        if row.geometry is None:
            continue

        properties = {
            "name": row.RaisonSociale,
            "category": row.LibelleCategorieAgregat,
            "type": row.LibelleSph,
            "maternite": row.maternite,
            "color": 'blue', # Embed color in properties
            "popup_html": f"<b>{row.RaisonSociale}</b><br>Catégorie: {row.LibelleCategorieAgregat}<br>Type: {row.LibelleSph}<br>Maternité: {row.maternite}"
        }

        etablissement = {
            "type": "Feature",
            "geometry": mapping(row.geometry), # Convert shapely Point to GeoJSON dict
            "properties": properties
        }

        geojson_features.append(etablissement)
    
    geojson_data = {
        "type": "FeatureCollection",
        "features": geojson_features
    }
    
    
    etablisssement = flm.GeoJson(
        geojson_data,
        marker=flm.Circle(
            radius=300,
            fill_color="blue",
            fill_opacity=1,
            stroke=False)
    )

    fg_sante.add_child(etablisssement)

    return fg_sante

def toggle_extra(fg, fg_name, key):
    if key:
        st.session_state["fg_extras_dict"][fg_name] = fg
    if (key == False) & (fg_name in st.session_state["fg_extras_dict"].keys()):
        st.session_state["fg_extras_dict"].pop(fg_name)

# Demo scenarii
demo_data = {
    'poids_emploi':None,
    'poids_logement':None,
    'poids_education':None,
    'poids_soutien':None,
    'poids_mobilité':None,
    'departement_actuel': None,
    'commune_actuelle':None,
    'loc_distance_km':None,
    'hebergement':None,
    'logement':None,
    'sante': None,
    'nb_adultes': None,
    'nb_enfants': None,
    'codes_metiers':None,
    'codes_formations':None,
    'classe_enfants':None,
    'binome_penalty':None
}

def run_demo(demo_data):
    if len(st.query_params) > 0:
        print("Mode démo")
        if st.query_params['demo'] == "1":
            print('demo 1')
            demo_data['departement_actuel'] = '33'
            demo_data['commune_actuelle'] = 'Bordeaux'
            demo_data['loc_distance_km'] = 25
            demo_data['hebergement'] = "Chez l'habitant"
            demo_data['nb_adultes'] = 1
            demo_data['nb_enfants'] = 0
            demo_data['poids_mobilité'] = 50
        if st.query_params['demo'] == "2":
            print('demo 2')
            demo_data['departement_actuel'] = '75'
            demo_data['commune_actuelle'] = 'Paris'
            demo_data['loc_distance_km'] = 25
            demo_data['hebergement'] = "Location"
            demo_data['logement'] = "Logement Social"
            demo_data['nb_adultes'] = 2
            demo_data['nb_enfants'] = 2
            demo_data['codes_metiers'] = [['B2X37', 'B2X38']]
            # demo_data['codes_metiers'] = [['Cuisiniers', 'Aides de cuisine et employés polyvalents de la restauration']]
            demo_data['codes_formations'] = [[],[331, 330, 326]]
            demo_data['classe_enfants'] = [0, 1] #index of ['Maternelle','Primaire','Collège','Lycée']
            demo_data['sante'] = "Maternité"
            demo_data['poids_mobilité'] = 0
        if st.sidebar.button('Exit Demo'):
            st.query_params.clear()
            st.rerun()
        
        # And we automatically load the results
        load_results(df=odis, scores_cat=scores_cat)
    
    return demo_data




# Loading and caching Datasets
t = performance_tracker(t, 'Start Dataset Import', timer_mode)
ODIS_FILE = '../csv/odis_june_2025_jacques.parquet'
SCORES_CAT_FILE = '../csv/odis_scores_cat.csv'
METIERS_FILE = '../csv/dares_nomenclature_fap2021.csv'
FORMATIONS_FILE = '../csv/index_formations.csv'
ECOLES_FILE = '../csv/annuaire_ecoles_france_mini.parquet'
MATERNITE_FILE = '../csv/annuaire_maternites_DREES.csv'
SANTE_FILE = '../csv/annuaire_sante_finess.parquet'

odis, codfap_index, codformations_index, annuaire_ecoles, annuaire_sante, scores_cat, coddep_set, depcom_df, codgeo_df, libfap_set, libform_set = init_datasets()
t = performance_tracker(t, 'End Dataset Import', timer_mode)

# Load all the session_states if they don't exist yet
session_states_init()


### BEGINNING OF THE STREAMLIT APP ###

# Sidebar
t = performance_tracker(t, 'Start App Sidebar', timer_mode)
with st.sidebar:
    st.logo('logo-jaccueille-singa.png', size='large')
    st.header("Application Prototype")
    st.subheader('Localisation Actuelle')
    # Département
    dep_index = coddep_set.index(demo_data['departement_actuel'] if demo_data['departement_actuel'] is not None else '33')
    departement_actuel = st.selectbox("Département", coddep_set, index=dep_index)
    # Commune
    communes = depcom_df[depcom_df.dep_code==departement_actuel]['libgeo']
    communes.reset_index(drop=True, inplace=True)
    com_index = int(communes[communes == demo_data['commune_actuelle']].index[0]) if demo_data['commune_actuelle'] is not None else 0
    commune_actuelle = st.selectbox("Commune", communes, index=com_index)
    commune_codgeo = codgeo_df[(codgeo_df.dep_code==departement_actuel) & (codgeo_df.libgeo==commune_actuelle)].index.item()
    # Distance
    st.subheader('Filtre Localisation')
    dist_value = demo_data['loc_distance_km'] if demo_data['loc_distance_km'] is not None else 10
    loc_distance_km = st.select_slider("Distance Max Relocalisation (en Km)", options=[10,25,50,100,500,1000], value=dist_value, disabled=False)


#Top filter Form
t = performance_tracker(t, 'Start Top Filters', timer_mode)
with st.container(border=False):
    st.subheader('Préférences de recherche')
    tab_foyer, tab_emploi, tab_formation, tab_logement, tab_edu, tab_sante, tab_autres, tab_ponderation = st.tabs(['Foyer', 'Emploi', 'Formation', 'Logement', 'Education', 'Santé', 'Autres', 'Pondération'])
            
    #Foyer
    with tab_foyer:
        col_left,col_right =st.columns(2)
        with col_left:
            nb_adultes_options = [1, 2]
            nb_adultes_index = nb_adultes_options.index(demo_data['nb_adultes']) if demo_data['nb_adultes'] is not None else 0
            nb_adultes = st.radio("Nombre d'adultes", options=nb_adultes_options, index=nb_adultes_index)
        with col_right:
            nb_enfants_value = demo_data['nb_enfants'] if demo_data['nb_enfants'] is not None else 0
            nb_enfants = st.select_slider("Nombre d'enfants", range(0,6), value=nb_enfants_value)

    #Metiers
    with tab_emploi:
        liste_metiers_adult=[]
        for adult in range(0,nb_adultes):
            try: 
                metiers_default = demo_data['codes_metiers'][adult]
            except (IndexError, TypeError): 
                metiers_default = []
            codfap_select = codfap_index[['Code FAP 341', 'Intitulé FAP 341']].set_index('Code FAP 341')
            # liste_metiers_adult += [st.multiselect("Métiers ciblés Adulte "+str(adult+1), libfap_set, default=metiers_default)]
            liste_metiers_adult += [st.multiselect("Métiers ciblés Adulte "+str(adult+1), codfap_select.index, format_func=lambda x: codfap_select.loc[x].item(), default=metiers_default)]

    #Formations
    with tab_formation:
        liste_formations_adult=[]
        for adult in range(0,nb_adultes):
            try:
                formations_default = demo_data['codes_formations'][adult]
            except (IndexError, TypeError):
                formations_default = [] 
            # liste_formations_adult += [st.multiselect("Formations ciblées Adulte "+str(adult+1),options=libform_set, default=formations_default)]
            liste_formations_adult += [st.multiselect("Métiers ciblés Adulte "+str(adult+1), codformations_index.index, format_func=lambda x: codformations_index.loc[x].item(), default=formations_default)]

    # Logement
    with tab_logement:
        hebergement_options = ["Chez l'habitant", 'Location', 'Foyer']
        hebergement_index = hebergement_options.index(demo_data['hebergement']) if demo_data['hebergement'] is not None else None
        hebergement = st.radio('Quel hébergement à court terme',options=hebergement_options, index=hebergement_index)
        logement_options = ['Location', 'Logement Social']
        logement_index = logement_options.index(demo_data['logement']) if demo_data['logement'] is not None else None
        logement = st.radio('Quel logement à long terme',options=logement_options, index=logement_index)
 
    #Education
    with tab_edu:
        liste_classe_enfants=[]
        if nb_enfants == 0:
            st.text("Aucun enfant n'a été ajouté dans l'onglet 'Foyer'.")
        else:
            col_left, col_right = st.columns(2)
            liste_classes = ['Maternelle','Primaire','Collège','Lycée']
            classe_index = 0
            with col_left:
                for enfant in range(0,nb_enfants, 2):
                    if demo_data['classe_enfants'] is not None:
                        try:
                            classe_index = demo_data['classe_enfants'][enfant] 
                        except IndexError:
                            classe_index = 0
                    liste_classe_enfants += [st.selectbox('Niveau enfant '+str(enfant+1), liste_classes, index=classe_index)]
            with col_right:
                for enfant in range(1,nb_enfants, 2):
                    if demo_data['classe_enfants'] is not None:
                        try:
                            classe_index = demo_data['classe_enfants'][enfant] 
                        except IndexError:
                            classe_index = 0
                    liste_classe_enfants += [st.selectbox('Niveau enfant '+str(enfant+1), liste_classes, index=classe_index)]

    #Santé
    with tab_sante:
        sante_options = ["Aucun", "Hopital", 'Maternité', "Centre Addictions & Maladies Mentales"]
        sante_index = sante_options.index(demo_data['sante']) if demo_data['sante'] is not None else 0
        besoin_sante = st.radio('Support médical à proximité',options=sante_options, index=sante_index)

    #Autres
    with tab_autres:
        st.text_input('Autres préférences')

    #Poids
    with tab_ponderation:
        col1, col2, col3 = st.columns(3)
        with col1:
            value_emploi = demo_data['poids_emploi'] if demo_data['poids_emploi'] is not None else 100
            poids_emploi = st.select_slider("Pondération Emploi", [0, 25, 50, 100], value=value_emploi)
            value_logement = demo_data['poids_logement'] if demo_data['poids_logement'] is not None else 100
            poids_logement = st.select_slider("Pondération Logement", [0, 25, 50, 100], value=value_logement)
        with col2:
            value_education = demo_data['poids_education'] if demo_data['poids_education'] is not None else 100
            poids_education = st.select_slider("Pondération Education", [0, 25, 50, 100], value=value_education)
            value_soutien = demo_data['poids_soutien'] if demo_data['poids_soutien'] is not None else 25
            poids_soutien = st.select_slider("Pondération Soutien", [0, 25, 50, 100], value=value_soutien)
        with col3:
            value_mobilite = demo_data['poids_mobilité'] if demo_data['poids_mobilité'] is not None else 100
            poids_mobilité = st.select_slider("Pondération Mobilité", [0, 25, 50, 100], value=value_mobilite)
            penalite_binome = st.select_slider("Décote binôme %", [0, 10, 25, 50, 100], value=25) / 100


# Bouton Pour lancer le scoring + affichage de la carte
st.button(
    "Afficher la carte" if st.session_state["processed_gdf"] is None else "Mettre à jour la carte",
    on_click=load_results, kwargs={'df':odis, 'scores_cat':scores_cat}, type="primary")
t = performance_tracker(t, 'Ready', timer_mode)

run_demo(demo_data)

# Main 2 sections with results and map
col_results, col_map = st.columns([1, 2])
t = performance_tracker(t, 'Start Results Column', timer_mode)

### Results Column
with col_results:
    
    if st.session_state['processed_gdf'] is not None:
        st.subheader("Meilleurs résultats")
        st.markdown('<style>di.st-key-button_top1 .stBUtton button div p  {text-align: left !important;color: red;}</style>',unsafe_allow_html=True)
        # if st.button("Afficher tous les meilleurs résultats sur la carte", type='tertiary'):
        #     for key, value in st.session_state["fg_dict_ref"].items():
        #         if key.startswith("Top"):
        #             show_fg(_fg_dict=st.session_state["fg_dict"], fg_key=value, clear_others=False)

        fg_dict_key = build_top_results(st.session_state["processed_gdf"], 5, st.session_state["prefs"])
            

t = performance_tracker(t, 'Start Map Column', timer_mode)
### Map Column
with col_map:
    
    if st.session_state['processed_gdf'] is not None:
        st.subheader("Carte Interactive")
        # we have scoring results let's draw the the map

        fg_dict = {}
        #scoring results map with shades + highlighted top 5 binomes 
        st.session_state['fg_dict_ref'] = show_scoring_results(
            st.session_state['processed_gdf'][['codgeo','libgeo','polygon','libgeo_binome', 'polygon_binome','weighted_score']],
            st.session_state['fg_dict_ref'], 
            5, 
            st.session_state['prefs']
        )

        # We now have a fg_dict_ref that looks like this:
        # {
        #     'Scores': fg_results,
        #     'Top1': fg_com1,
        #     'Top2': fg_com1,
        #     'Top...': fg_com...,
        #     'TopN': fg_comN
        # }

        # We default to the 'Scores' which shows all the scored results in shaded green and selected Geo in blue
        st.session_state['fg_dict']['Scores'] = st.session_state['fg_dict_ref']['Scores'] 


        # We add additional informational layers
        # We keep the schools in the same academy only
        # See doc here: https://python-visualization.github.io/folium/latest/user_guide/geojson/geojson_marker.html
        col1, col2 = st.columns(2)
        with col1:
            t = performance_tracker(t, 'Start Ecoles Display', timer_mode)
            if st.session_state['prefs']['nb_enfants'] > 0:
                st.session_state['fg_ecoles'] = build_local_ecoles_layer(st.session_state["selected_geo"], annuaire_ecoles, st.session_state["prefs"])
                # st.checkbox('Afficher Ecoles', key='afficher_ecoles', value=False, on_change=toggle_extra, kwargs={'fg':st.session_state['fg_ecoles'], 'fg_name':'ecoles', 'key':st.session_state['afficher_ecoles']})
                # checkbox_name = 'Afficher ' + ', '.join(st.session_state['prefs']['classe_enfants'])
                checkbox_name = 'Afficher établissements scolaires'
                st.checkbox(checkbox_name, key='afficher_ecoles', value=False)
                # print(st.session_state['fg_ecoles'])
                toggle_extra(fg=st.session_state['fg_ecoles'], fg_name='ecoles', key=st.session_state['afficher_ecoles'])
            t = performance_tracker(t, 'End Ecoles Display', timer_mode)

        with col2:
            t = performance_tracker(t, 'Start Sante Display', timer_mode)
            if st.session_state['prefs']['besoin_sante'] != "Aucun":
                st.session_state['fg_sante'] = build_local_sante_layer(st.session_state["selected_geo"], annuaire_sante, st.session_state["prefs"])
                # st.checkbox('Afficher Etablissements de Santé', key='afficher_sante', value=False, on_change=toggle_extra, kwargs={'fg':st.session_state['fg_sante'], 'fg_name':'sante', 'key':st.session_state['afficher_sante']})
                # checkbox_name = 'Afficher ' + st.session_state['prefs']['besoin_sante']
                checkbox_name = 'Afficher établissements de santé'
                st.checkbox(checkbox_name, key='afficher_sante', value=False)
                toggle_extra(fg=st.session_state['fg_sante'], fg_name='sante', key=st.session_state['afficher_sante'])
            t = performance_tracker(t, 'End Sante Display', timer_mode)
            # st.write(st.session_state["fg_extras_dict"])

        t = performance_tracker(t, 'Start Map Display', timer_mode)

        m = odis_base_map(st.session_state['selected_geo'], st.session_state['prefs'])
        # print(st.session_state['fg_dict'])
        # st.write(list(st.session_state['fg_dict'].values())+list(st.session_state["fg_extras_dict"].values()))
        # st_data = 
        st_folium(
            m,
            zoom=st.session_state["zoom"],
            feature_group_to_add=list(st.session_state['fg_dict'].values())+list(st.session_state["fg_extras_dict"].values()),
            width=1000,
            height=800,
            key="odis_scored_map",
            use_container_width=True,
            returned_objects=[],
            # layer_control=flm.LayerControl(collapsed=False)
        )
        t = performance_tracker(t, 'End Map Display', timer_mode)


# st.write(st.session_state['processed_gdf'])