import time
print('################### RE-RUN #########################')

def performance_tracker(t, text, timer_mode):
    if timer_mode:
        print(str(round(time.time()-t,2))+'|'+text)
        return time.time()
t = time.time()
print(time.ctime(t))
timer_mode = True

t = performance_tracker(t, 'Start Import', timer_mode)


from pandas import concat 
import numpy as np
import geopandas as gpd
from shapely.geometry import mapping
import streamlit as st
import folium as flm
from folium.plugins import FastMarkerCluster
from plotly.express import line_polar
from streamlit_folium import st_folium
from branca.colormap import linear
from odis_stream2_scoring import compute_odis_score, init_loading_datasets

t = performance_tracker(t, 'End Import', timer_mode)

st.set_page_config(layout="wide", page_title='Odis Stream2 Prototype')
st.markdown(
    """
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
    """,
    unsafe_allow_html=True
) # This loads the font awesome icons that we use in the map legend

# st.write(':red[Dev In Progress...]')

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
    if 'prefs' not in st.session_state:
        st.session_state["prefs"] = {}
    if "processed_gdf" not in st.session_state:
        st.session_state["processed_gdf"] = None
    if "selected_geo" not in st.session_state:
        st.session_state["selected_geo"] = None
    if "highlighted_result" not in st.session_state:
        st.session_state["highlighted_result"] = [False,None,None,None] # This will be the tuple where we will keep track of highlighted result (selected, row, index, prefs)
    if 'fg_ecoles' not in st.session_state:
        st.session_state['fg_ecoles'] = False
    if 'fg_sante' not in st.session_state:
        st.session_state['fg_sante'] = False
    if 'fg_services' not in st.session_state:
        st.session_state['fg_services'] = False
    if 'fg_dict_ref' not in st.session_state:
        st.session_state["fg_dict_ref"] = {}
    if 'fgs_to_show' not in st.session_state:
        st.session_state['fgs_to_show'] = set()
    if 'pitch' not in st.session_state:
        st.session_state["pitch"] = []
    if "zoom" not in st.session_state:
        st.session_state["zoom"] = 10
    if "center" not in st.session_state:
        st.session_state["center"] = None
    if "besoins_autres" not in st.session_state:
        st.session_state["besoins_autres"] = {}

@st.cache_resource
def init_datasets():
    # We load all the datasets
    odis, scores_cat, codfap_index, codformations_index, annuaire_ecoles, annuaire_sante, annuaire_inclusion, incl_index, plan_sncf = init_loading_datasets(
        ODIS_FILE, 
        SCORES_CAT_FILE, 
        METIERS_FILE, 
        FORMATIONS_FILE, 
        ECOLES_FILE, 
        MATERNITE_FILE, 
        SANTE_FILE, 
        INCLUSION_FILE,
        SNCF_FILE
        )
    
    coddep_set = sorted(set(odis['dep_code']))
    depcom_df = odis[['dep_code','libgeo']].sort_values('libgeo')
    codgeo_df = odis[['dep_code','libgeo']]
    libfap_set = sorted(set(codfap_index['Intitulé FAP 341']))
    libform_set = sorted(set(codformations_index['libformation']))
    
    return odis, codfap_index, codformations_index, annuaire_ecoles, annuaire_sante, annuaire_inclusion, incl_index, scores_cat, coddep_set, depcom_df, codgeo_df, libfap_set, libform_set, plan_sncf

# Scoring et affichage de la carte avec tous les résultats
@st.cache_data
def compute_score(_df, scores_cat, prefs, _incl_index):
    return compute_odis_score(_df, scores_cat, prefs, _incl_index)

def set_prefs(scores_cat):

    prefs = {
        'poids_emploi':poids_emploi,
        'poids_logement':poids_logement,
        'poids_education':poids_education,
        'poids_inclusion':poids_inclusion,
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
        'besoins_autres': st.session_state['besoins_autres'],
        'binome_penalty':penalite_binome,
        'pop_min': pop_min
    }
    # Add binomes scores & weights to scores_cat
    scores_cat_prefs=scores_cat.copy()
    for index, row in scores_cat.iterrows():
        if row.loc['incl_binome']:
            row_to_add = scores_cat.loc[[index]]
            row_to_add['score'] = row_to_add['score'] + '_binome'
            row_to_add['score_name'] = row_to_add['score_name'] + ' (Binôme)'
            row_to_add['incl_binome'] = False
            scores_cat_prefs = concat([scores_cat_prefs, row_to_add])
    scores_cat_prefs['weight'] = scores_cat_prefs['cat'].apply(lambda x: prefs['poids_'+x])
    scores_cat_prefs.set_index('score', inplace=True)

    return prefs, scores_cat_prefs

def load_results(df, scores_cat): 
    print(' <---------------------> Click Afficher Résultats <--------------------->')
    prefs, scores_cat_prefs = set_prefs(scores_cat) # We update the prefs with the latest inputs
    
    # Computing Weighted Scores given a source dataframe with geo info and a scoring categorisation
    odis_scored = compute_score(
        _df=df,
        scores_cat=scores_cat,
        prefs=prefs,
        _incl_index=incl_index,
        )
    
    # We pop the selected commune from the results
    selected_geo = odis_scored.loc[prefs['commune_actuelle']]
    odis_scored = odis_scored.drop(prefs['commune_actuelle'])
    
    # We sort so that we can have the top results by selecting the first rows
    odis_scored = odis_scored.sort_values('weighted_score', ascending=False).reset_index()

    # We reset a bunch of session states as we reload the results
    st.session_state['scores_cat'] = scores_cat_prefs
    st.session_state['processed_gdf'] = odis_scored
    st.session_state['selected_geo'] = selected_geo
    st.session_state['prefs'] = prefs
    st.session_state["center"] =[selected_geo.polygon.centroid.y, selected_geo.polygon.centroid.x]
    st.session_state['fg_ecoles'] = False
    st.session_state['fg_sante'] = False
    st.session_state['fg_services'] = False
    st.session_state['fg_dict_ref'] = {}
    st.session_state["highlighted_result"] = [False, None, None, None]

def result_highlight(row, index, prefs):
    # This is to highlight one specific result the user clicked on
    # First we clear up all the other TopN fg (if any)
    st.session_state['fgs_to_show'] = {k for k in st.session_state['fgs_to_show'] if not k.startswith('Top')}

    if (st.session_state["highlighted_result"][0]) and (index == st.session_state["highlighted_result"][2]):
        # This means the user clicked on the same button again --> intends to collapse
        st.session_state["highlighted_result"] = [False, None, None, None] #reset
            
    else:
        # Add the red outline to show the commune on the scored communes map
        fg_key = 'Top'+str(index+1)
        st.session_state['fgs_to_show'].add(fg_key)
        # We center the map and zoom on that result
        st.session_state["center"] = [row.polygon.centroid.y, row.polygon.centroid.x]
        st.session_state["zoom"] = 11
        st.session_state["highlighted_result"] = [True, row, index, prefs] # We keep track of which resulted was currently highlighted

def build_top_results(_df, n, prefs):
    # Buils the list of Top n results as expander-like buttons
    for index, row in _df.head(n).iterrows():
        # We show the proposed commune with a solid red border and binome with dashed one (if any)
        fg = flm.FeatureGroup(name="Commune Top"+str(index+1))
        geojson_features = []

        # Top 5 with individual feature groups for each commune+binome pair
        if row.polygon is not None:

            geojson_geometry = mapping(row.polygon)

            # style_to_use = styling_communes(style='style_target')
            style_to_use = {
                "color": "red",
                "fillOpacity": 0,
                "weight": 3,
                "opacity": 1,
                "html":'<div style="width: 10px;height: 10px;border: 1px solid black;border-radius: 5px;background-color: orange;">1</div>'
            }

            properties = {
                "Nom": row.libgeo,
                "Score": str(int(row.weighted_score*100))+'%',
                "style": style_to_use,
                "popup_html": ( # Pre-render popup HTML for convenience
                    f"<b>{row.libgeo}</b><br>"
                    f"Score: {row.weighted_score*100:.0f}%<br>"
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

                # style_to_use = styling_communes(style='style_binome')
                style_to_use = {
                    "color": "red",
                    "fillOpacity": 0,
                    "weight": 2,
                    "opacity": 1,
                    "dashArray": "5, 5",
                }
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
            fg.add_child(commune_json)
        
        # We register this fg into our refence dict of all possible fgs
        fg_key = 'Top'+str(index+1)
        st.session_state['fg_dict_ref'][fg_key] = fg


        # Then we can show the expander-like button (= expander with on-click and)
        title = f"Top {index+1} | {row.libgeo}"# + (f" (en binôme avec {row.libgeo_binome})" if row.binome else "")
        st.button(
            title,
            on_click=result_highlight,
            kwargs={'row':row, 'index':index, 'prefs':prefs},
            use_container_width=True,
            key='button_top'+str(index+1),
            type='primary',
            icon=":material/arrow_drop_down:"
            )
        
        # Because of streamlit limitations we can't use the st.expander so we mimic one (we can't know the state open/close of expander)
        # If we have clicked on the button, we generate the detailed report for that specific result and we want to display it below the corresponding button
        if st.session_state["highlighted_result"][0]:
            if index == st.session_state["highlighted_result"][2]:
                with st.container(border=True):
                    # We print the Markdown pitch generated when we click on the button
                    st.session_state['pitch'] = produce_pitch_markdown(row=row, prefs=st.session_state["prefs"], scores_cat=st.session_state['scores_cat'], codfap_index=codfap_index, codformations_index=codformations_index)
                    st.markdown(st.session_state['pitch'])

                    # Radar Chart that shows categories scores
                    data_to_plot = row[[col for col in row.index if col.endswith('_cat_score')]]
                    data_to_plot.rename(lambda x: x.split('_')[0], inplace=True)
                    fig = line_polar(theta=data_to_plot.index.str.capitalize(), r=data_to_plot.values * 100, line_close=True)
                    fig.update_traces(fill='toself')
                    st.plotly_chart(fig)
                    st.caption('Plus le critère s’approche du bord du cercle, plus il est attractif dans cette zone')

                    st.divider()
                    # Additional info about the communes that are not related to the search
                    st.text('Plus d’informations sur cette localité :')
                    with st.expander('Top 10 des métiers recherchés'):
                        liste_metiers=[]
                        if row.be_libfap_top is not None:
                            for item in row.be_libfap_top:
                                liste_metiers.append(f'- {item}')
                        st.markdown("\n".join(liste_metiers))
                    
                    with st.expander('Formations proposées'):
                        liste_formations=[]
                        if row.noms_formations is not None:
                            for item in row.noms_formations:
                                liste_formations.append(f'- {item}')
                        
                        #Let's includes the formations offered by the binome (if any)
                        try:
                            formations_binome = _df[_df.codgeo == row.codgeo_binome].noms_formations.item()
                        except:
                            print(f"{_df.codgeo}| Could not fetch binome's formations")
                        else: 
                            if formations_binome is not None:
                                for item in formations_binome:
                                    liste_formations.append(f'- {item}')
                            st.markdown("\n".join(set(liste_formations)))
                    
                    with st.expander("Services d'inclusions proposés"):                
                        services = annuaire_inclusion[annuaire_inclusion.codgeo == row.codgeo]
                        liste_services=[]
                        if services is not None:
                            categories = sorted(set(services.categorie))
                            for categorie in categories:
                                liste_services += [f"- **{categorie.replace('-', ' ').capitalize()}**"]
                                for item in services.itertuples():
                                    if item.categorie == categorie and item.service != '-':
                                        liste_services += [f"    - {item.service.replace('-', ' ').capitalize()}"]
                        st.markdown("\n".join(liste_services))
                        print("\n".join(liste_services))

                    st.markdown(f"[Page OD&IS de la commune](https://jaccueille-indicateurs-commune.wedodata.dev/territoire/presentation/accueil/{row.codgeo})")
                    st.markdown(f"[Page Wikipedia de la commune]({row.url_wikipedia})")
            
    # This is used as a callback so we don't return anything

def produce_pitch_markdown(row, prefs, scores_cat, codfap_index, codformations_index):
    # We produce the pitch for the top N results
    # We generate a markdown text that is natively displayed by streamlit
    pitch_md = []
    population = "{:,.0f}".format(row["population"])
    pitch_md.append(f'**{row["libgeo"]}** ({population.replace(","," ")} habitants) fait partie de l\'agglomération: {row["epci_nom"]}.  ')
    
    if row["binome"]:
        pitch_md.append(f'\nCette commune, en [binôme](# "Lorsque des communes sont proposées en binômes, c’est qu’ensemble elles correspondent au projet de vie de la personne. L’une peut présenter des opportunités d’emplois, l’autre de logements. Une installation pourrait alors être envisagée aux alentours de leur délimitation commune.") avec sa voisine **{row["libgeo_binome"]}**, pourrait être intéressante {"pour "+demo_data["nom"] if demo_data["nom"] is not None else ""}. La correspondance de ses besoins et opportunités est évaluée à **{row["weighted_score"] *100:.0f}**% ')
    else:
        pitch_md.append(f'\nCette commune pourrait être intéressante pour la personne accompagnée. La correspondance de ses besoins et opportunités est évaluée à **{row["weighted_score"] *100:.0f}**% ')


    # Adding the top contributing criterias
    crit_scores_col = [col for col in row.keys() if '_scaled' in col]
    
    # First compute the weighted (+ penalty for binome) for each criteria score
    weighted_row=row.copy()
    for col in crit_scores_col:
        weight = scores_cat.loc[col]['weight'].item()
        weighted_row[col] =  row[col] * weight * (1-prefs['binome_penalty']) if col.endswith('_binome') else row[col] * weight
    # Then we sort an print the top 5
    weighted_row = weighted_row[crit_scores_col].dropna().sort_values(ascending=False)

    pitch_md.append(f"\nCette commune, ou ce binôme de communes, comporte en effet :")
    for i in range(0, 5):
        if weighted_row.iloc[i] > 50:
            score = weighted_row.index[i]
            score_affichage = scores_cat.loc[score]['score_affichage']
            score_metric = scores_cat.loc[score]['metric']
            score_unit = scores_cat.loc[score]['unit']
            display_factor = scores_cat.loc[score]['display_factor']
            show_metric = scores_cat.loc[score]['show_metric']
            if show_metric:
                pitch_md.append(f'- {score_affichage} ({display_factor*row[score_metric]:.0f} {score_unit})')
            else:
                pitch_md.append(f'- {score_affichage}')

            #Let's handle the case where we want to display the list of matched items (if any)
            if '_match_' in score:
                adult = score[-8:][:1] # e.g. met_match_code_adult1_scaled --> 1
                matched_names = []
                if score.startswith('met'): #metiers
                    matched_names += list(codfap_index[codfap_index['Code FAP 341'].isin(row['met_match_codes_adult'+str(adult)])]['Intitulé FAP 341'])
                elif score.startswith('form'): #formations
                    matched_names += list(codformations_index[codformations_index.index.isin(row['form_match_codes_adult'+str(adult)])]['libformation'])
                else:
                    continue
                if matched_names:
                    for name in matched_names:
                        pitch_md.append(f'    - {name}')

    return "\n".join(pitch_md)

@st.cache_data
def show_scoring_results(_df, prefs):
    t = time.time()
    t = performance_tracker(t, 'Start show_scoring_results', timer_mode)


    # We show all the communes in the search radius in a shaded greeen according to their scores
    fg_results = flm.FeatureGroup(name="Scores")

    # We pass all the scored communes at once inside a geojson feature group
    geojson_features = []
    score_weights_dict = st.session_state["processed_gdf"].set_index("codgeo")["weighted_score"]
    colormap = linear.YlGn_09.scale(score_weights_dict.min(), score_weights_dict.max())
    # colormap.caption = "Score"

    # All results colored based on score
    t = performance_tracker(t, 'Start row iterations', timer_mode)
    for row in _df.itertuples(index=False):
       
        if row.polygon is None:
            continue
    
        geojson_geometry = mapping(row.polygon)
        
        style_to_use = {
            "fillColor": colormap(score_weights_dict.get(row.codgeo)),
            "fillOpacity": 0.8,
            "stroke": False,
            "weight": 1,
            "color": "grey"
        }

        properties = {
            "Nom": row.libgeo,
            "Score": str(int(row.weighted_score*100))+'%',
            "style": style_to_use,
            "popup_html": ( # Pre-render popup HTML for convenience
                f"<b>{row.libgeo}</b><br>"
                f"Score: {row.weighted_score*100:.0f}%<br>"
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
    t = performance_tracker(t, 'Start add current geo', timer_mode)
    geojson_geometry = mapping(st.session_state['selected_geo'].polygon)

    style_to_use = {
        "fillColor": 'blue',
        "fillOpacity": 0.5,
        "stroke": True
    }

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
    
    
    t = performance_tracker(t, 'Start wrapup everything in feature collection', timer_mode)
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
        popup=flm.GeoJsonPopup(fields=['popup_html'], aliases=[''], localize=True, parse_html=True) # Use pre-rendered HTML
    )
    
    fg_results.add_child(communes)
    t = performance_tracker(t, 'End show_scoring_results', timer_mode)

    return fg_results, colormap

@st.cache_data
def odis_base_map(_current_geo, prefs):
    # We store the last known prefs in session to avoid reload
    gs = gpd.GeoSeries(_current_geo.polygon, crs='EPSG:2154')
    center_loc = [
            gs.centroid.y.iloc[0],
            gs.centroid.x.iloc[0]
        ]
    st.session_state["center"] = center_loc
        # 10,25,50,100,500,1000
    match prefs['loc_distance_km']:
        case 10:
            zoom_start = 11
        case 25:
            zoom_start = 10
        case 50:
            zoom_start = 8
        case 100:
            zoom_start = 7
        case 500:
            zoom_start = 6
        case 1000:
            zoom_start = 5

    st.session_state["zoom"] = zoom_start
    m = flm.Map(location=center_loc, zoom_start=10)

    return m

@st.cache_data
def build_legend(items_list):
    # This builds a legend that will be displayed on top of the map with st.markdown(legend_html)
    # https://github.com/pointhi/leaflet-color-markers
    leaflet_colors = {
        "red": "#D63E2A",
        "blue": "#38A9DC",
        "green": "#72B026",
        "purple": "#5B396B",
        "violet": "#D252B9",
        "orange": "#F69730",
        "darkred": "#A23336",
        "lightred": "#EB7D7F",
        "darkblue": "#0066A2",
        "darkgreen": "#728224",
        "teal": "#436978",
        "grey": "#A3A3A3"
    }
    
    legend_html = """
            <div id='maplegend' class='maplegend' 
                style='position: absolute; z-index: 9999; background-color: rgba(255, 255, 255, 0.7);
                border-radius: 6px; padding: 5px; font-size: 11px; right: 5px; top: 20px;'>     
            <div class='legend-scale'>
            <ul class='legend-labels'>
            <li><span>Score: Faible</span><span class='scale'></span><span> Elevé</span></li>
        """

    for item in items_list:
        legend_html += f"""
            <li><span class='marker-circle' style='background:{leaflet_colors.get(item['color'])}');'><i class='fa fa-{item['icon']}' style='color:white'></i></span><span>{item['text']}</span></li>
        """

    legend_html += """
            </ul>
            </div>
            </div> 
            <style type='text/css'>
            .maplegend .legend-scale ul {margin: 0; padding: 0; color: #0f0f0f;}
            .maplegend .legend-scale ul li {list-style: none; line-height: 18px; margin: 0px;}
            .maplegend ul.legend-labels li span {height: 16px; margin-right: 5px; vertical-align:middle}
            .maplegend ul.legend-labels li .scale {display: inline-block; width:100px; background: linear-gradient(90deg,rgba(250, 241, 157, 1) 0%, rgba(67, 153, 100, 1) 100%)}
            .maplegend ul.legend-labels li .marker-circle {display: inline-flex; width:22px; height:22px; border-radius:50%; margin: 2px}
            .maplegend ul.legend-labels li .marker-circle i {padding:6px}
            </style>
        """
    return legend_html


# Affichage Support Education
@st.cache_data
def filter_ecoles(_current_geo, annuaire_ecoles, prefs):
    # we consider all the etablissements scolaires in the target codgeos and the ones around (voisins)
    target_codgeos = set(
        st.session_state['processed_gdf'].codgeo.tolist() 
        # +[x for y in st.session_state['processed_gdf'].codgeo_voisins.tolist() for x in y] #Need to fix crash when no voisin exist
        )
    niveaux_enfants = set(liste_classe_enfants)
    # We have 3 cases here
    #   Maternelle without elementaire
    #   Elementaire without maternelle
    #   Elementaire with maternelle

    maternelle_df = None
    elementaire_df = None
    primaire_df = None 
    college_df = None
    lycee_df = None

    if 'Maternelle' in niveaux_enfants:
        maternelle_df = annuaire_ecoles[(annuaire_ecoles.ecole_maternelle > 0) & (annuaire_ecoles.ecole_elementaire == 0)].copy()
        maternelle_df['type']='maternelle'
        primaire_df = annuaire_ecoles[(annuaire_ecoles.ecole_maternelle > 0) & (annuaire_ecoles.ecole_elementaire > 0)].copy()
        primaire_df['type']='primaire' #primaire = maternelle + elementaire

    if 'Elémentaire' in niveaux_enfants:
        elementaire_df = annuaire_ecoles[(annuaire_ecoles.ecole_maternelle == 0) & (annuaire_ecoles.ecole_elementaire > 0)].copy()
        elementaire_df['type']='elementaire'
        primaire_df = annuaire_ecoles[(annuaire_ecoles.ecole_maternelle > 0) & (annuaire_ecoles.ecole_elementaire > 0)].copy()
        primaire_df['type']='primaire' #primaire = maternelle + elementaire
   
    if 'Collège' in niveaux_enfants:
        college_df = annuaire_ecoles[annuaire_ecoles.type_etablissement == 'Collège'].copy()
        college_df['type']='college'

    if 'Lycée' in niveaux_enfants:
        lycee_df = annuaire_ecoles[annuaire_ecoles.type_etablissement == 'Lycée'].copy()
        lycee_df['type']='lycee'
    
    
    annuaire_ecoles = concat([maternelle_df, primaire_df, college_df, lycee_df])
    mask = annuaire_ecoles['code_commune'].isin(target_codgeos)
    filtered_ecoles=annuaire_ecoles[mask]
    
    return filtered_ecoles

@st.cache_data
def build_local_ecoles_layer(_current_geo, _annuaire_ecoles, prefs):
    etablissements_scolaire_colors = {
        'maternelle': 'purple',
        'elementaire': 'orange',
        'primaire': 'green',
        'college': 'blue',
        'lycee': 'red',
        'default': 'grey'
        }
    
    callback = """
    function (row) {
        var icon, marker;
        icon = L.AwesomeMarkers.icon({
            icon: "pencil", markerColor: row[2]});
        marker = L.marker(new L.LatLng(row[0], row[1]));
        marker.setIcon(icon);
        
        var tooltip_text = "<b>"+row[3]+"</b><br>Type: "+row[4]+"<br>Maternelle: "+row[5];
        marker.bindTooltip(tooltip_text);
        
        var popup_text = "<b>"+row[3]+"</b><br>Type: "+row[4]+"<br>Maternelle: "+row[5];
        marker.bindPopup(popup_text);

        return marker;
    };
    """
    fg_ecoles = flm.FeatureGroup(name="Établissements Scolaires")
    
    filtered_ecoles = _annuaire_ecoles[_annuaire_ecoles.type_etablissement.isin(['Ecole', 'Collège', 'Lycée'])]
    filtered_ecoles = filter_ecoles(_current_geo, filtered_ecoles, prefs)
    
    # Now let's add these schools to the map in the fg_ecoles feature group
    filtered_ecoles = gpd.GeoDataFrame(filtered_ecoles, geometry='geometry', crs='EPSG:4326')
    # We remove ecoles with empty and None Geometries
    filtered_ecoles = filtered_ecoles[~filtered_ecoles.is_empty]
    filtered_ecoles = filtered_ecoles[filtered_ecoles.notna()]
    
    # We add necessary columns to properly display markers and their tooltups
    filtered_ecoles['marker_color'] = filtered_ecoles.apply(lambda x: etablissements_scolaire_colors.get(x.type, 'default'), axis=1)
    filtered_ecoles['maternelle_presente'] = np.where(filtered_ecoles.ecole_maternelle > 0, "Oui", "Non")

    locations = list(zip(
        filtered_ecoles.geometry.y, 
        filtered_ecoles.geometry.x, 
        filtered_ecoles.marker_color,
        filtered_ecoles.nom_etablissement,
        filtered_ecoles.type_etablissement,
        filtered_ecoles.maternelle_presente
        ))

    FastMarkerCluster(locations, callback=callback, showCoverageOnHover=False).add_to(fg_ecoles)
    
    return fg_ecoles


# Affichage Support Santé
@st.cache_data
def filter_sante(target_codgeos, _annuaire_sante, prefs):
    # we consider all the etablissements soclaires in the target codgeos and the ones around (voisins)
    filtered_sante = _annuaire_sante[_annuaire_sante['codgeo'].isin(target_codgeos)].copy()
    filtered_sante['type'] = None # Here we track the type of etablissement for further use

    if prefs['besoin_sante'] == 'Maternité':
        filtered_sante = filtered_sante[filtered_sante.maternite]
        filtered_sante['type'] = 'maternite'
    elif prefs['besoin_sante'] == "Hopital":
        cat_list = ['355', '362', '101', '106']
        filtered_sante = filtered_sante[filtered_sante.Categorie.isin(cat_list)]
        filtered_sante['type'] = 'hopital'
    elif prefs['besoin_sante'] == "Soutien Psychologique & Addictologie":
        cat_list = ['156', '292', '425', '412', '366', '415', '430', '444']
        filtered_sante = filtered_sante[filtered_sante.Categorie.isin(cat_list)]
        filtered_sante['type'] = 'addiction_maladies_mentales'
    
    return filtered_sante[['nofinesset', 'codgeo', 'RaisonSociale', 'LibelleCategorieAgregat', 'LibelleSph', 'geometry', 'maternite', 'type']]

@st.cache_data
def build_local_sante_layer(_current_geo, _annuaire_sante, prefs):
    fg_sante = flm.FeatureGroup(name="Établissements de Santé")
    
    target_codgeos = set(
        st.session_state['processed_gdf'].codgeo.tolist() 
        # +[x for y in st.session_state['processed_gdf'].codgeo_voisins.tolist() for x in y]
        )
    
    etablissements_sante_colors = {
        'maternite':'orange',
        'hopital':'lightblue',
        'addiction_maladies_mentales':'purple',
        'default':'grey'
    }

    filtered_annuaire = filter_sante(target_codgeos, _annuaire_sante, prefs)
    filtered_annuaire = gpd.GeoDataFrame(filtered_annuaire, geometry='geometry', crs='EPSG:2154')
    filtered_annuaire.to_crs(epsg=4326, inplace=True)
    
    # We remove ecoles with empty and None Geometries
    filtered_annuaire = filtered_annuaire[~filtered_annuaire.is_empty]
    filtered_annuaire = filtered_annuaire[filtered_annuaire.notna()]


    callback = """
    function (row) {
        var icon, marker;
        icon = L.AwesomeMarkers.icon({
            icon: "plus", markerColor: row[2]});
        marker = L.marker(new L.LatLng(row[0], row[1]));
        marker.setIcon(icon);
        
        var tooltip_text = "<b>"+row[3]+"</b><br>Maternité: "+row[6];
        marker.bindTooltip(tooltip_text);
        
        var popup_text = "<b>"+row[3]+"</b><br>Catégorie: "+row[4]+"<br>Type: "+row[5]+"<br>Maternité: "+row[6];
        marker.bindPopup(popup_text);

        return marker;
    };
    """
    
    # We add necessary columns to properly display markers and their tooltups
    filtered_annuaire['marker_color'] = filtered_annuaire.apply(lambda x: etablissements_sante_colors.get(x.type, 'default'), axis=1)
    filtered_annuaire['maternite_presente'] = np.where(filtered_annuaire.maternite, "Oui", "Non")

    locations = list(zip(
        filtered_annuaire.geometry.y, 
        filtered_annuaire.geometry.x, 
        filtered_annuaire.marker_color,
        filtered_annuaire.RaisonSociale,
        filtered_annuaire.LibelleCategorieAgregat,
        filtered_annuaire.LibelleSph,
        filtered_annuaire.maternite_presente
        ))

    FastMarkerCluster(locations, callback=callback, showCoverageOnHover=False).add_to(fg_sante)

    return fg_sante


# Affichage Services d'Inclusion (Autres Besoins)
@st.cache_data
def build_local_services_layer(_current_geo, _annuaire_inclusion, prefs):
    fg_services = flm.FeatureGroup(name="Services d'inclusion")

    target_codgeos = set(
        st.session_state['processed_gdf'].codgeo.tolist() 
        # +[x for y in st.session_state['processed_gdf'].codgeo_voisins.tolist() for x in y]
        )
    
    services_inclusion_colors = {
        'famille':'orange',
        'numerique':'grey',
        'acces-aux-drois-et-citoyennete':'purple',
        'illetrisme':'darkgreen',
        'apprendre-francais':'blue',
        'sante':'red',
        'default':'grey'
    }
    
    filtered_annuaire = _annuaire_inclusion[_annuaire_inclusion['codgeo'].isin(target_codgeos)]
    filtered_annuaire = filtered_annuaire[
        (filtered_annuaire.categorie.isin(st.session_state['prefs']['besoins_autres'].keys()))
        # & (filtered_annuaire.service.isin([x for x in st.session_state['prefs']['besoins_autres'].values()]))
        ]
    filtered_annuaire = gpd.GeoDataFrame(filtered_annuaire, geometry='geometry', crs='EPSG:4326')
    
    # We remove ecoles with empty and None Geometries
    filtered_annuaire = filtered_annuaire[~filtered_annuaire.is_empty]
    filtered_annuaire = filtered_annuaire[filtered_annuaire.notna()]


    callback = """
    function (row) {
        var icon, marker;
        icon = L.AwesomeMarkers.icon({
            icon: "heart", markerColor: row[2]});
        marker = L.marker(new L.LatLng(row[0], row[1]));
        marker.setIcon(icon);
        
        var tooltip_text = "<b>"+row[3]+"</b><br>Catégorie: "+row[4]+"<br>Service: "+row[5];
        marker.bindTooltip(tooltip_text);
        
        var popup_text = "<b>"+row[3]+"</b><br>Catégorie: "+row[4]+"<br>Service: "+row[5]+"<br>Description: "+row[6];
        marker.bindPopup(popup_text);

        return marker;
    };
    """
    
    # We add necessary columns to properly display markers and their tooltups
    filtered_annuaire['marker_color'] = filtered_annuaire.apply(lambda x: services_inclusion_colors.get(x.categorie, 'default'), axis=1)

    locations = list(zip(
        filtered_annuaire.geometry.y, 
        filtered_annuaire.geometry.x, 
        filtered_annuaire.marker_color,
        filtered_annuaire.nom,
        filtered_annuaire.categorie.replace('-', ' ').str.capitalize(),
        filtered_annuaire.service.replace('-', ' ').str.capitalize(),
        filtered_annuaire.presentation_resume
        ))

    FastMarkerCluster(locations, callback=callback, showCoverageOnHover=False).add_to(fg_services)

    return fg_services

# Load Demo data
def load_demo_data(demo_data):
    if len(st.query_params) > 0:
        print("Mode démo")
        if st.query_params['demo'] == "1":
            print('demo 1')
            demo_data['nom'] = 'Zacharie'
            demo_data['departement_actuel'] = '33'
            demo_data['commune_actuelle'] = 'Bordeaux'
            demo_data['loc_distance_km'] = 1 #[0=25km, 1=50km, 2=France]
            demo_data['hebergement'] = "Chez l'habitant"
            demo_data['nb_adultes'] = 1
            demo_data['nb_enfants'] = 0
            demo_data['poids_mobilité'] = 50
        if st.query_params['demo'] == "2":
            print('demo 2')
            demo_data['nom'] = 'Olga & Dimitri'
            demo_data['departement_actuel'] = '75'
            demo_data['commune_actuelle'] = 'Paris'
            demo_data['loc_distance_km'] = 2 #[0=25km, 1=50km, 2=France]
            demo_data['hebergement'] = "Location"
            demo_data['logement'] = "Logement Social"
            demo_data['nb_adultes'] = 2
            demo_data['nb_enfants'] = 2
            demo_data['codes_metiers'] = [['B2X37', 'B2X38']]
            # demo_data['codes_metiers'] = [['Cuisiniers', 'Aides de cuisine et employés polyvalents de la restauration']]
            demo_data['codes_formations'] = [[],[331, 330, 326]]
            demo_data['classe_enfants'] = [0, 1] #index of ['Maternelle','Elémentaire','Collège','Lycée']
            demo_data['sante'] = "Maternité"
            demo_data['poids_mobilité'] = 0
        if st.query_params['demo'] == "3":
            print('demo 3')
            demo_data['nom'] = 'Aïcha'
            demo_data['departement_actuel'] = '13'
            demo_data['commune_actuelle'] = 'Marseille'
            demo_data['loc_distance_km'] = 1 #[0=25km, 1=50km, 2=France]
            demo_data['hebergement'] = "Location"
            demo_data['logement'] = "Logement Social"
            demo_data['nb_adultes'] = 1
            demo_data['nb_enfants'] = 2
            demo_data['codes_metiers'] =[['T2A60']] #[['S1X20', 'S1X40', 'S1X80']]
            # demo_data['codes_metiers'] = [['Cuisiniers', 'Aides de cuisine et employés polyvalents de la restauration']]
            # [[],[330, 324]] # Spécialités plurivalentes sanitaires et sociales + Secrétariat
            demo_data['classe_enfants'] = [1, 2] #index of ['Maternelle','Elémentaire','Collège','Lycée']
            demo_data['besoins_autres'] = {'apprendre-francais':['-']}
            demo_data['poids_mobilité'] = 50
            demo_data['poids_inclusion'] = 50
            demo_data['poids_emploi'] = 100

        if st.sidebar.button('Quitter Mode Démo', key='quit_demo', type='tertiary'):
            st.query_params.clear()
            st.rerun()
        st.markdown('<style> .st-key-quit_demo {position:relative; top:90vh}<style>', unsafe_allow_html=True) # Displays the button at the bottom of the sidebar
        
        # And we automatically load the results
        # load_results(df=odis, scores_cat=scores_cat)
    
    # print(demo_data)
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
INCLUSION_FILE = '../csv/odis_services_incl_exploded.parquet'
SNCF_FILE = '../csv/formes-des-lignes-du-rfn.geojson'

odis, codfap_index, codformations_index, annuaire_ecoles, annuaire_sante, annuaire_inclusion, incl_index, scores_cat, coddep_set, depcom_df, codgeo_df, libfap_set, libform_set, plan_sncf = init_datasets()
t = performance_tracker(t, 'End Dataset Import', timer_mode)



# Load all the session_states if they don't exist yet
session_states_init()

demo_data_default = { #we reset unless a demo scenarii is passed as as a query parameter e.g. /?demo=1
    'nom': None,
    'poids_emploi':None,
    'poids_logement':None,
    'poids_education':None,
    'poids_inclusion':None,
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
    'binome_penalty':None,
    'besoins_autres': None
}
demo_data = load_demo_data(demo_data_default)

### BEGINNING OF THE STREAMLIT APP ###

# Sidebar
t = performance_tracker(t, 'Start App Sidebar', timer_mode)
with st.sidebar:
    st.image('logo-jaccueille-singa.png', width=None)
    # st.header("Recherche d'un nouveau lieu de vie selon son projet")
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
    
    st.divider()
    with st.expander('Pondérations des critères'):
        value_education = demo_data['poids_education'] if demo_data['poids_education'] is not None else 100
        poids_education = st.select_slider("Pondération Education", [0, 25, 50, 100], value=value_education)
        
        value_emploi = demo_data['poids_emploi'] if demo_data['poids_emploi'] is not None else 100
        poids_emploi = st.select_slider("Pondération Prejet Professionnel", [0, 25, 50, 100], value=value_emploi)
        
        value_logement = demo_data['poids_logement'] if demo_data['poids_logement'] is not None else 100
        poids_logement = st.select_slider("Pondération Logement", [0, 25, 50, 100], value=value_logement)

        value_soutien = demo_data['poids_inclusion'] if demo_data['poids_inclusion'] is not None else 25
        poids_inclusion = st.select_slider("Pondération Soutien à l'inclusion", [0, 25, 50, 100], value=value_soutien)

        value_mobilite = demo_data['poids_mobilité'] if demo_data['poids_mobilité'] is not None else 100
        poids_mobilité = st.select_slider("Pondération Mobilité", [0, 25, 50, 100], value=value_mobilite)

        penalite_binome = st.select_slider("Décote binôme %", [1, 10, 25, 50, 100], value=50) / 100
        pop_min = st.select_slider("Population Minimum", [0, 500, 1000, 5000, 10000], value=1000)


#Top filter Form
t = performance_tracker(t, 'Start Top Filters', timer_mode)
with st.container(border=False, key='top_menu'):
    st.markdown("""
                <style>
                    .st-key-top_menu  {background-color:whitesmoke; padding:30px; border-radius:10px}
                    .stTabs div div button div p {font-size:1rem}
                </style>
                """
                , unsafe_allow_html=True)
    if demo_data['nom'] is not None:
        st.subheader(f"Situation et besoins de {demo_data['nom']}")
    else:
        st.subheader(f"Situation et besoins")

    col_intro, col_button = st.columns([4,1])

    with col_intro:
        st.text(f"Retrouvez ici toutes les informations liées à la situation de la personne que vous accompagnez. \nA tout moment, vous pouvez les modifier et actualiser la recherche.")
    
    # Bouton Pour lancer le scoring + affichage de la carte
    with col_button: 
        st.button(
            "Lancer la recherche" if st.session_state["processed_gdf"] is None else "Mettre à jour la carte",
            key='afficher_carte_btn',
            on_click=load_results, kwargs={'df':odis, 'scores_cat':scores_cat}, type="primary"
            )

    # Tabs
    tab_foyer, tab_edu, tab_emploi, tab_logement, tab_sante, tab_autres, tab_mobilite= st.tabs(['Situation familiale', 'Education', 'Projet Professionnel', 'Logement', 'Santé', 'Autres Besoins', 'Mobilité'])
            
    #Foyer
    with tab_foyer:
        col_left,col_right =st.columns(2)
        with col_left:
            nb_adultes_options = [1, 2]
            nb_adultes_index = demo_data['nb_adultes']-1 if demo_data['nb_adultes'] is not None else 0
            nb_adultes = st.radio("Nombre d'adultes", options=nb_adultes_options, index=nb_adultes_index, horizontal=True)
        with col_right:
            nb_enfants_value = demo_data['nb_enfants'] if demo_data['nb_enfants'] is not None else 0
            nb_enfants = st.radio("Nombre d'enfants", [0,1,2,3,4,5], index=nb_enfants_value, horizontal=True)

    #Education
    with tab_edu:
        liste_classe_enfants=[]
        if nb_enfants == 0:
            st.text("Aucun enfant n'a été ajouté dans l'onglet 'Foyer'.")
        else:
            col_left, col_right = st.columns(2)
            liste_classes = ['Maternelle','Elémentaire','Collège','Lycée']
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

    # Projet Professionel
    with tab_emploi:
        col_emploi, col_formations = st.columns(2)
        
        #Metiers
        with col_emploi:
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
        with col_formations:
            liste_formations_adult=[]
            for adult in range(0,nb_adultes):
                try:
                    formations_default = demo_data['codes_formations'][adult]
                except (IndexError, TypeError):
                    formations_default = [] 
                # liste_formations_adult += [st.multiselect("Formations ciblées Adulte "+str(adult+1),options=libform_set, default=formations_default)]
                liste_formations_adult += [st.multiselect("Formations recherchées Adulte "+str(adult+1), codformations_index.index, format_func=lambda x: codformations_index.loc[x].item(), default=formations_default)]

    #Mobilité
    with tab_mobilite:
        distance_options = {
            # 10:'Extrêmement Important (~10km)',
            25:'Important (~25km)',
            50:'Assez important (~50km)',
            # 100:'Peu Important (~100km)',
            1000:'Toute la France'
        }
        dist_value = demo_data['loc_distance_km'] if demo_data['loc_distance_km'] is not None else 0
        loc_distance_km = st.radio(label='Attachement au lieu de vie actuel :', options=distance_options.keys(), format_func=lambda x: distance_options.get(x), index=dist_value)

    #Logement
    with tab_logement:
        hebergement_options = ["Chez l'habitant", 'Location', 'Foyer']
        hebergement_index = hebergement_options.index(demo_data['hebergement']) if demo_data['hebergement'] is not None else None
        hebergement = st.radio('Quel hébergement à court terme',options=hebergement_options, index=hebergement_index)
        logement_options = ['Location', 'Logement Social']
        logement_index = logement_options.index(demo_data['logement']) if demo_data['logement'] is not None else None
        logement = st.radio('Quel logement à long terme',options=logement_options, index=logement_index)

    #Santé
    with tab_sante:
        sante_options = ["Aucun", "Hopital", 'Maternité', "Soutien Psychologique & Addictologie"]
        sante_index = sante_options.index(demo_data['sante']) if demo_data['sante'] is not None else 0
        besoin_sante = st.radio('Support médical à proximité',options=sante_options, index=sante_index)

    #Autres
    with tab_autres:
        st.text("Sélectionnez d'autres besoins:")
        col1, col2 = st.columns(2)
        with col1:
            cat = None
            cat = st.selectbox('Catégorie', sorted(set(annuaire_inclusion.categorie)), format_func=lambda x: x.replace('-', ' ').capitalize(), index=2)
            service = st.selectbox('Service', sorted(set(annuaire_inclusion[annuaire_inclusion.categorie == cat].service)), format_func=lambda x: x.replace('-', ' ').capitalize(), index=0)
            if st.button('Ajouter', type='secondary'):
                if cat in st.session_state["besoins_autres"]:
                    st.session_state["besoins_autres"][cat].append(service)
                else:
                    st.session_state["besoins_autres"][cat] = [service]
        with col2:
            st.text('Besoins ajoutés:')
            # st.write(st.session_state["besoins_autres"])
            if demo_data['besoins_autres'] is not None:
                st.session_state["besoins_autres"] = demo_data['besoins_autres']
            if bool(st.session_state["besoins_autres"]) == False:
                st.text('   Aucun')
            else:
                for key, values in st.session_state["besoins_autres"].items():    
                    for value in values:
                        st.text(f"[{key.replace('-', ' ').capitalize()}] {value.replace('-', ' ').capitalize()}")

        st.text_input('Autres préférences (champ libre)')
 
    t = performance_tracker(t, 'Ready', timer_mode)


# Main two sections: results and map
col_results, col_map = st.columns([2, 3])


### Results Column
t = performance_tracker(t, 'Start Results Column', timer_mode)
with col_results:
    # st.write(st.session_state['processed_gdf'])
    if st.session_state['processed_gdf'] is not None:
        st.subheader("Meilleurs résultats")
        if demo_data["nom"] is not None:
            st.text(f'Voici des localités qui pourraient convenir à {demo_data["nom"]}')
        else:
            st.text(f'Voici des localités qui pourraient convenir pour ce projet de vie')
        st.markdown('<style>[class*="st-key-button_top"] .stButton button div {text-align:left; width:100%;}</style>', unsafe_allow_html=True)

        # We build the list of top 5 results
        build_top_results(st.session_state["processed_gdf"], 5, st.session_state["prefs"])
        
        

### Map Column
t = performance_tracker(t, 'Start Map Column', timer_mode)
with col_map:

    if st.session_state['processed_gdf'] is not None:
        # st.subheader("Carte Interactive")
        # we have scoring results let's draw the the map
        
        #scoring results layer with shades + highlighted top 5 binomes 
        st.session_state['fg_dict_ref']['Scores'], colormap = show_scoring_results(
                st.session_state['processed_gdf'][['codgeo','libgeo','polygon','libgeo_binome', 'polygon_binome','weighted_score']],
                st.session_state['prefs']
            )
        st.session_state['fgs_to_show'].add('Scores')


        
        col1, col2 = st.columns([1,4], vertical_alignment='center')
        with col1:
            st.text("Afficher:")
        
        with col2:
            # st.write(st.session_state["highlighted_result"])
            with st.container(key='display_toggles'):
                st.markdown('<style>.st-key-display_toggles {gap:0rem}</style>',unsafe_allow_html=True)
                if st.checkbox("Les 5 meilleurs résultats sur la carte"):
                    for key, value in st.session_state["fg_dict_ref"].items():
                        if key.startswith("Top"):
                            st.session_state['fgs_to_show'].add(key)
                    st.session_state["zoom"] = None           
                elif st.session_state["highlighted_result"][0]: # We hihglight the existing result
                    # result_highlight(st.session_state["highlighted_result"][1], st.session_state["highlighted_result"][2], st.session_state["highlighted_result"][3])
                    st.session_state['fgs_to_show'] = {k for k in st.session_state['fgs_to_show'] if not k.startswith('Top')}
                    fg_key = 'Top'+str(st.session_state["highlighted_result"][2]+1)
                    st.session_state['fgs_to_show'].add(fg_key)
                else: # we clear all
                    st.session_state['fgs_to_show'] = {k for k in st.session_state['fgs_to_show'] if not k.startswith('Top')}
            
                # We add additional informational layers
                legend_items = []

                # ECOLES
                t = performance_tracker(t, 'Start Ecoles Display', timer_mode)
                if st.session_state['prefs']['nb_enfants'] > 0:
                    key_extra = 'fg_ecoles'
                    if st.checkbox(
                        'Les établissements scolaires pertinents', 
                        key=key_extra, 
                        value=False, 
                    ):
                        st.session_state['fg_dict_ref'][key_extra] = build_local_ecoles_layer(st.session_state["selected_geo"], annuaire_ecoles, st.session_state["prefs"])
                        st.session_state['fgs_to_show'].add(key_extra)
                        # Légende
                        icon = 'pencil'
                        if 'Maternelle' in st.session_state['prefs'].get('classe_enfants'):
                            legend_items.append({'color':'violet', 'icon':icon, 'text':'Maternelles'})
                            legend_items.append({'color':'green', 'icon':icon, 'text':'Ecoles Primaires'})
                        if 'Elémentaire' in st.session_state['prefs'].get('classe_enfants'):
                            legend_items.append({'color':'orange', 'icon':icon, 'text':'Ecoles Elémentaires'})
                            legend_items.append({'color':'green', 'icon':icon, 'text':'Ecoles Primaires'})
                        if 'Collège' in st.session_state['prefs'].get('classe_enfants'):
                            legend_items.append({'color':'blue', 'icon':icon, 'text':'Collèges'})
                        if 'Lycée' in st.session_state['prefs'].get('classe_enfants'):
                            legend_items.append({'color':'red', 'icon':icon, 'text':'Lycées'})
                    else:
                        st.session_state['fgs_to_show'].discard(key_extra)
                            
                t = performance_tracker(t, 'End Ecoles Display', timer_mode)

                # SANTE
                t = performance_tracker(t, 'Start Sante Display', timer_mode)
                if st.session_state['prefs']['besoin_sante'] != "Aucun":
                    key_extra = 'fg_sante'
                    st.session_state['fg_dict_ref'][key_extra] = build_local_sante_layer(st.session_state["selected_geo"], annuaire_sante, st.session_state["prefs"])
                    if st.checkbox(
                        'Les établissements de santé pertinents', 
                        key=key_extra, 
                        value=False, 
                    ):
                        st.session_state['fgs_to_show'].add(key_extra)
                        # Légende
                        icon = 'plus'
                        if 'Maternité' in st.session_state['prefs'].get('besoin_sante'):
                            legend_items.append({'color':'orange', 'icon':icon, 'text':'Maternité'})
                        if 'Hopital' in st.session_state['prefs'].get('besoin_sante'):
                            legend_items.append({'color':'blue', 'icon':icon, 'text':'Hopitaux'})
                        if 'Soutien Psychologique & Addictologie' in st.session_state['prefs'].get('besoin_sante'):
                            legend_items.append({'color':'violet', 'icon':icon, 'text':'Centres Addictions & Santé Mentale'})
                        
                    else:
                        st.session_state['fgs_to_show'].discard(key_extra)
                            
                t = performance_tracker(t, 'End Sante Display', timer_mode)

                # SERVICES INCLUSION
                t = performance_tracker(t, 'Start Services Inclusion Display', timer_mode)
                if bool(st.session_state['prefs']['besoins_autres']):
                    key_extra = 'fg_services'
                    st.session_state['fg_dict_ref'][key_extra] = build_local_services_layer(st.session_state["selected_geo"], annuaire_inclusion, st.session_state["prefs"])
                    if st.checkbox(
                        "Les services d'inclusion pertinents", 
                        key=key_extra, 
                        value=False, 
                    ):
                        st.session_state['fgs_to_show'].add(key_extra)
                        # Légende
                        icon = 'heart'
                        if 'famille' in st.session_state['besoins_autres']:
                            legend_items.append({'color':'orange', 'icon':icon, 'text':'Famille'})
                        if 'numerique' in st.session_state['besoins_autres']:
                            legend_items.append({'color':'grey', 'icon':icon, 'text':'Numérique'})
                        if 'acces-aux-droits' in st.session_state['besoins_autres']:
                            legend_items.append({'color':'violet', 'icon':icon, 'text':'Accès aux droits'})
                        if 'illetrisme' in st.session_state['besoins_autres']:
                            legend_items.append({'color':'darkgreen', 'icon':icon, 'text':'Illetrisme'})
                        if 'apprendre-francais' in st.session_state['besoins_autres']:
                            legend_items.append({'color':'blue', 'icon':icon, 'text':'Apprendre le français'})
                        if 'sante' in st.session_state['besoins_autres']:
                            legend_items.append({'color':'red', 'icon':icon, 'text':'Santé'})
                    else:
                        st.session_state['fgs_to_show'].discard(key_extra)

                t = performance_tracker(t, 'End Services Inclusions Display', timer_mode)

                # LIGNES SNCF
                # t = performance_tracker(t, 'Start SNCF Display', timer_mode)
                # if st.checkbox(
                #     "Réseau de train", 
                #     value=False, 
                # ):
                #     fg_sncf = flm.FeatureGroup(name="SNCF")

                #     # We pass all the scored communes at once inside a geojson feature group
                #     geojson_features = []
                #     loc_actuelle_gdf = st.session_state['processed_gdf'][st.session_state['processed_gdf'].codgeo == st.session_state["prefs"].get('commune_actuelle')]
                #     loc_actuelle_gdf = loc_actuelle_gdf.to_crs(epsg=2154)
                #     plan_sncf_to_show = plan_sncf.sjoin_nearest(loc_actuelle_gdf, max_distance=1000*st.session_state["prefs"].get('loc_distance_km'))
                #     st.text(plan_sncf_to_show.shape)
                #     for row in plan_sncf_to_show.itertuples(index=False):
                    
                #         if row.geometry is None:
                #             continue
                    
                #         geojson_geometry = mapping(row.geometry)
                        
                #         # style_to_use = {
                #         #     # "fillColor": colormap(score_weights_dict.get(row.codgeo)),
                #         #     # "fillOpacity": 0.8,
                #         #     "stroke": True,
                #         #     "weight": 1,
                #         #     "color": "blue"
                #         # }

                #         properties = {
                #             # "Nom": row.libgeo,
                #             # "Score": str(int(row.weighted_score*100))+'%',
                #             # "style": style_to_use,
                #             # "popup_html": ( # Pre-render popup HTML for convenience
                #             #     f"<b>{row.libgeo}</b><br>"
                #             #     f"Score: {row.weighted_score*100:.0f}%<br>"
                #             #     f"Binôme: {row.libgeo_binome}"
                #             # )
                #         }

                #         voie = {
                #             "type": "Feature",
                #             "geometry": geojson_geometry,
                #             "properties": properties
                #         }
                #         geojson_features.append(voie)

                #     geojson_data = {
                #         "type": "FeatureCollection",
                #         "features": geojson_features
                #     }

                #     ligne_sncf = flm.GeoJson(
                #         geojson_data,
                #         name="Voies SNCF",
                #         # style_function=style_function,
                #         # tooltip=flm.GeoJsonTooltip(fields=['Nom', 'Score'], aliases=['Nom', 'Score']),
                #         # popup=flm.GeoJsonPopup(fields=['popup_html'], aliases=[''], localize=True, parse_html=Trfue) # Use pre-rendered HTML
                #     )
                    
                #     fg_sncf.add_child(ligne_sncf)
                #     st.session_state['fg_dict_ref']['fg_sncf'] = fg_sncf
                #     st.session_state['fgs_to_show'].add('fg_sncf')

                        # Légende
                        # icon = 'heart'
                        # if 'famille' in st.session_state['besoins_autres']:
                        #     legend_items.append({'color':'orange', 'icon':icon, 'text':'Famille'})
                        # if 'numerique' in st.session_state['besoins_autres']:
                        #     legend_items.append({'color':'grey', 'icon':icon, 'text':'Numérique'})
                        # if 'acces-aux-droits' in st.session_state['besoins_autres']:
                        #     legend_items.append({'color':'violet', 'icon':icon, 'text':'Accès aux droits'})
                        # if 'illetrisme' in st.session_state['besoins_autres']:
                        #     legend_items.append({'color':'darkgreen', 'icon':icon, 'text':'Illetrisme'})
                        # if 'apprendre-francais' in st.session_state['besoins_autres']:
                        #     legend_items.append({'color':'blue', 'icon':icon, 'text':'Apprendre le français'})
                        # if 'sante' in st.session_state['besoins_autres']:
                        #     legend_items.append({'color':'red', 'icon':icon, 'text':'Santé'})
                # else:
                #     st.session_state['fgs_to_show'].discard('fg_sncf')

                # t = performance_tracker(t, 'End SNCF Display', timer_mode)

                # Légende
                legend = build_legend(legend_items)
                st.markdown(legend, unsafe_allow_html=True)

        # Affichage de la carte (toujours en dernier)
        t = performance_tracker(t, 'Start Map Display', timer_mode)
        
        # Base Map
        m = odis_base_map(st.session_state['selected_geo'], st.session_state['prefs'])

        # FeatureGroups
        # We now have a fg_dict_ref that looks like this:
        # {
        #     'Scores': fg_results,
        #     'Top1': fg_com1,
        #     'Top2': fg_com1,
        #     'Top...': fg_com...,
        #     'fg_ecoles': fg_ecoles,
        #      ...
        # }
        
        # Now we can build the list of feature groups to display (fgs_list) based on:
        # 1. The list of feature groups names we want to show (fgs_to_show)
        # 2. The reference dict of all available fgs (fg_dict_ref)
        
        fgs_list = []
        # print(st.session_state['fgs_to_show'])
        for fg_name in st.session_state['fgs_to_show']:
            if fg_name == 'Scores':
                # We always insert Scores as the base (bottom) layer to improve visibility
                fgs_list.insert(0, st.session_state['fg_dict_ref'].get(fg_name))
            else:
                fgs_list.append(st.session_state['fg_dict_ref'].get(fg_name))
        
        st_folium(
            m,
            zoom=st.session_state["zoom"],
            center=st.session_state["center"],
            feature_group_to_add=fgs_list, #list(st.session_state['fg_dict'].values()),#list(st.session_state['fg_dict'].values()),
            key="odis_scored_map",
            use_container_width=True,
            returned_objects=[],
            # layer_control=flm.LayerControl(collapsed=False)
        )
        st.markdown('<style>.stCustomComponentV1   {border-radius:10px}</style>', unsafe_allow_html=True) # Rounded corners for the map widget
        t = performance_tracker(t, 'End Map Display', timer_mode)

if st.session_state['processed_gdf'] is not None:
    st.sidebar.divider()
    if st.sidebar.button('Export des résultats', icon=':material/picture_as_pdf:', type='secondary'):
        st.cache_data.clear()

# st.text('Pour Développement')
# st.write(st.session_state['fg_dict_ref'])
# st.write(st.session_state['fg_dict'])
# st.write(st.session_state['processed_gdf'])
# cols_to_show = [col for col in st.session_state['processed_gdf'].columns if (col.endswith('scaled')) or (col.endswith('_score'))]
# st.write(st.session_state['processed_gdf'][['libgeo']+cols_to_show])
# st.write(st.session_state['scores_cat'])
# st.write(st.session_state['prefs'])
