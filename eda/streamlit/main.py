import copy
import time

import streamlit as st

# Local imports
from scoring import compute_odis_score, load_all_datasets
import config as cfg
import ui
import maps

print(f"--- App re-run at {time.ctime(time.time())} ---")

st.set_page_config(layout="wide", page_title='Odis')
st.markdown(
    """
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
    """,
    unsafe_allow_html=True
) # This loads the font awesome icons that we use in the map legend

### INIT OF THE STREAMLIT APP ###

def session_states_init(defaults):
    """Initializes all necessary keys in Streamlit's session state."""
    # App data and results
    if 'app_data' not in st.session_state:
        st.session_state['app_data'] = {}
    if 'config' not in st.session_state:
        st.session_state['config'] = None
    if "processed_gdf" not in st.session_state:
        st.session_state['processed_gdf'] = None
    if "selected_geo" not in st.session_state:
        st.session_state['selected_geo'] = None
    
    # Map state
    if "highlighted_result" not in st.session_state:
        st.session_state['highlighted_result'] = [False, None]
    if 'fg_dict_ref' not in st.session_state:
        st.session_state['fg_dict_ref'] = {}
    if 'fgs_to_show' not in st.session_state:
        st.session_state['fgs_to_show'] = set()
    if "zoom" not in st.session_state:
        st.session_state['zoom'] = 10
    if "center" not in st.session_state:
        st.session_state['center'] = cfg.DEFAULT_MAP_CENTER

    # Initialize UI state from default config
    ui_keys_map = {
        'ui_departement': 'departement_actuel',
        'ui_commune': 'commune_actuelle',
        'ui_poids_education': 'poids_education',
        'ui_poids_emploi': 'poids_emploi',
        'ui_poids_logement': 'poids_logement',
        'ui_poids_inclusion': 'poids_inclusion',
        'ui_poids_mobilité': 'poids_mobilité',
        'ui_penalite_binome': ('binome_penalty', lambda x: int(x * 100)),
        'ui_pop_min': 'pop_min',
        'ui_nb_adultes': 'nb_adultes',
        'ui_nb_enfants': 'nb_enfants',
        'ui_loc_distance_km': 'loc_distance_km',
        'ui_hebergement': 'hebergement',
        'ui_logement': 'logement',
        'ui_besoin_sante': 'sante',
        'ui_besoins_autres': 'besoins_autres'
    }

    for ui_key, config_key in ui_keys_map.items():
        if ui_key not in st.session_state:
            if isinstance(config_key, tuple):
                base_key, transform = config_key
                st.session_state[ui_key] = transform(defaults[base_key])
            else:
                st.session_state[ui_key] = defaults[config_key]

    # Initialize dynamic UI keys
    for i in range(2): # Max 2 adults
        if f'ui_metiers_adult_{i}' not in st.session_state:
            st.session_state[f'ui_metiers_adult_{i}'] = []
        if f'ui_formations_adult_{i}' not in st.session_state:
            st.session_state[f'ui_formations_adult_{i}'] = []
    for i in range(5): # Max 5 children
        if f'ui_classe_enfant_{i}' not in st.session_state:
            st.session_state[f'ui_classe_enfant_{i}'] = 'Maternelle'

@st.cache_resource
def init_datasets():
    """Loads all datasets and returns them in a structured dictionary."""
    print("--- Loading all datasets... ---")
    odis, scores_cat, codfap_index, codformations_index, annuaire_ecoles, annuaire_sante, annuaire_inclusion, incl_index, plan_sncf = load_all_datasets(
        cfg.ODIS_FILE,
        cfg.SCORES_CAT_FILE,
        cfg.METIERS_FILE,
        cfg.FORMATIONS_FILE,
        cfg.ECOLES_FILE,
        cfg.MATERNITE_FILE,
        cfg.SANTE_FILE,
        cfg.INCLUSION_FILE,
        cfg.SNCF_FILE
        )
    return {
        "odis": odis,
        "scores_cat": scores_cat,
        "codfap_index": codfap_index,
        "codformations_index": codformations_index,
        "annuaire_ecoles": annuaire_ecoles,
        "annuaire_sante": annuaire_sante,
        "annuaire_inclusion": annuaire_inclusion,
        "incl_index": incl_index,
        "plan_sncf": plan_sncf,
        "coddep_set": sorted(set(odis['dep_code'])),
        "depcom_df": odis[['dep_code','libgeo']].sort_values('libgeo'),
    }

# Scoring et affichage de la carte avec tous les résultats
@st.cache_data
def run_scoring_pipeline(_df_original, scores_cat, config, _incl_index):
    """Wrapper for the scoring function to enable Streamlit caching."""
    return compute_odis_score(_df_original, scores_cat, config, _incl_index)

def run_search():
    """
    Callback function for the 'Lancer la recherche' button.
    It creates the config, runs the scoring, and updates the session state.
    """
    print('--- Running new search ---')
    config = ui.create_scoring_config_from_inputs()
    st.session_state['config'] = config

    # Run the main scoring pipeline
    odis_scored = run_scoring_pipeline(
        _df_original=st.session_state.app_data['odis'],
        scores_cat=st.session_state.app_data['scores_cat'],
        config=config,
        _incl_index=st.session_state.app_data['incl_index'],
    )

    # Pop the current commune from the results and store it separately
    selected_geo = st.session_state.app_data['odis'].loc[[config.commune_actuelle]].copy()
    odis_scored = odis_scored.drop(config.commune_actuelle, errors='ignore')

    # Sort results by score
    odis_scored = odis_scored.sort_values('weighted_score', ascending=False).reset_index()

    # Reset session state for the new results
    st.session_state['processed_gdf'] = odis_scored
    st.session_state['selected_geo'] = selected_geo
    st.session_state['center'] = [selected_geo.polygon.centroid.y.iloc[0], selected_geo.polygon.centroid.x.iloc[0]]
    st.session_state['zoom'] = maps.get_map_zoom(config.loc_distance_km)
    st.session_state['fg_dict_ref'] = {}
    st.session_state['highlighted_result'] = [False, None]

# Load Demo data
def load_demo_data(demo_data):
    """Loads demo data if a 'demo' query parameter is present in the URL."""
    if len(st.query_params) > 0:
        demo_id = st.query_params.get('demo')
        if demo_id in cfg.DEMO_SCENARIOS:
            print(f"--- Loading Demo Mode {demo_id} ---")
            demo_data.update(cfg.DEMO_SCENARIOS[demo_id])

        if st.sidebar.button('Quitter Mode Démo', key='quit_demo'):
            st.query_params.clear()
            st.rerun()
        st.markdown('<style> .st-emotion-cache-16txtl3 {position:relative; top:80vh}</style>', unsafe_allow_html=True)

    return demo_data

# --- Main App Execution ---
defaults = cfg.DEMO_DATA_DEFAULT
session_states_init(defaults)

# Load all datasets and cache them
st.session_state.app_data = init_datasets()

# Handle demo data from URL query params
demo_data = load_demo_data(copy.deepcopy(cfg.DEMO_DATA_DEFAULT))

### BEGINNING OF THE STREAMLIT APP ###

# Sidebar
with st.sidebar:
    st.image('logo-jaccueille-singa.png', width=None)
    ui.display_sidebar(demo_data)

#Top filter Form
with st.container(border=False, key='top_menu'):
    st.markdown("""
                <style>
                    .st-key-top_menu  {background-color:whitesmoke; padding:30px; border-radius:10px}
                    .stTabs div div button div p {font-size:1rem}
                </style>
                """
                , unsafe_allow_html=True)

    ui.display_main_header(demo_data.get('nom'))

    col_intro, col_button = st.columns([4,1])
    with col_intro:
        st.text("Renseignez les informations liées au projet de vie. Vous pouvez les modifier à tout moment.")
    with col_button: 
        st.button(
            "Lancer la recherche" if st.session_state["processed_gdf"] is None else "Mettre à jour la carte",
            on_click=run_search, type="primary"
            )

    # Input Tabs
    ui.display_input_tabs(demo_data)

# Main two sections: results and map
col_results, col_map = st.columns([2, 3])

### Results Column
with col_results:
    if st.session_state['processed_gdf'] is not None:
        ui.display_results_list(demo_data.get('nom'))

### Map Column
with col_map:
    from streamlit_folium import st_folium
    if st.session_state['processed_gdf'] is not None:
        # Base layer with all scored communes
        st.session_state['fg_dict_ref']['Scores'], colormap = maps.build_scores_layer(st.session_state['processed_gdf'])
        st.session_state['fgs_to_show'].add('Scores')

        col1, col2 = st.columns([1,4], vertical_alignment='center')
        with col1:
            st.text("Afficher:")
        with col2:
            with st.container(key='display_toggles'):
                st.markdown('<style>.st-key-display_toggles {gap:0rem}</style>',unsafe_allow_html=True)
                if st.checkbox("Les 5 meilleurs résultats sur la carte"):
                    for key, value in st.session_state["fg_dict_ref"].items():
                        if key.startswith("Top"):
                            st.session_state['fgs_to_show'].add(key)
                    st.session_state["zoom"] = None           
                elif st.session_state["highlighted_result"][0]: # A result is highlighted, keep it visible
                    st.session_state['fgs_to_show'] = {k for k in st.session_state['fgs_to_show'] if not k.startswith('Top')}
                    fg_key = f'Top{st.session_state["highlighted_result"][1] + 1}'
                    st.session_state['fgs_to_show'].add(fg_key)
                else: # Clear all top results highlights
                    st.session_state['fgs_to_show'] = {k for k in st.session_state['fgs_to_show'] if not k.startswith('Top')}
            
                # We add additional informational layers
                legend_items = []
                config = st.session_state['config']
                target_codgeos = set(st.session_state['processed_gdf'].codgeo.tolist())

                # ECOLES
                if config.nb_enfants > 0 and st.checkbox('Établissements scolaires'):
                    st.session_state['fg_dict_ref']['fg_ecoles'] = maps.build_ecoles_layer(st.session_state.app_data['annuaire_ecoles'], target_codgeos, config)
                    st.session_state['fgs_to_show'].add('fg_ecoles')
                    legend_items.append({'color': 'green', 'icon': 'pencil', 'text': 'Écoles'})
                else:
                    st.session_state['fgs_to_show'].discard('fg_ecoles')

                # SANTE
                if config.besoin_sante != "Aucun" and st.checkbox('Établissements de santé'):
                    st.session_state['fg_dict_ref']['fg_sante'] = maps.build_sante_layer(st.session_state.app_data['annuaire_sante'], target_codgeos, config)
                    st.session_state['fgs_to_show'].add('fg_sante')
                    legend_items.append({'color': 'blue', 'icon': 'plus', 'text': 'Santé'})
                else:
                    st.session_state['fgs_to_show'].discard('fg_sante')

                # SERVICES INCLUSION
                if config.besoins_autres and st.checkbox("Services d'inclusion"):
                    st.session_state['fg_dict_ref']['fg_services'] = maps.build_services_layer(st.session_state.app_data['annuaire_inclusion'], target_codgeos, config)
                    st.session_state['fgs_to_show'].add('fg_services')
                    legend_items.append({'color': 'purple', 'icon': 'heart', 'text': 'Inclusion'})
                else:
                    st.session_state['fgs_to_show'].discard('fg_services')

                # Légende
                legend = maps.build_legend(legend_items)
                st.markdown(legend, unsafe_allow_html=True)

        # Affichage de la carte (toujours en dernier)
        # Base Map
        m = maps.create_base_map(st.session_state["center"], st.session_state["zoom"])

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
        # Add selected feature groups to the map
        fgs_to_add = [
            st.session_state['fg_dict_ref'][name] 
            for name in sorted(list(st.session_state['fgs_to_show'])) # Sort to ensure consistent layer order
            if name in st.session_state['fg_dict_ref']
        ]

        st_folium(
            m,
            zoom=st.session_state["zoom"],
            center=st.session_state["center"],
            feature_group_to_add=fgs_to_add,
            key="odis_scored_map",
            use_container_width=True,
            returned_objects=[],
        )
        st.markdown('<style>.stCustomComponentV1   {border-radius:10px}</style>', unsafe_allow_html=True) # Rounded corners for the map widget

if st.session_state['processed_gdf'] is not None:
    st.sidebar.divider()
    if st.sidebar.button('Export des résultats', icon=':material/picture_as_pdf:', type='secondary'):
        st.cache_data.clear()
