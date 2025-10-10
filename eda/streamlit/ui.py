# /home/jacques/odis/13_odis/eda/streamlit/ui.py
import streamlit as st
import pandas as pd
from plotly.express import line_polar

import config as cfg
import maps

def display_sidebar(demo_data: dict):
    """Displays the sidebar with location and weight controls."""
    st.subheader('Localisation Actuelle')
    
    # --- Location ---
    app_data = st.session_state.app_data
    dep_index = app_data['coddep_set'].index(demo_data.get('departement_actuel', '33'))
    departement_actuel = st.selectbox("Département", app_data['coddep_set'], index=dep_index, key="ui_departement")
    
    communes = app_data['depcom_df'][app_data['depcom_df'].dep_code == departement_actuel]['libgeo']
    communes.reset_index(drop=True, inplace=True)
    try:
        com_index = int(communes[communes == demo_data.get('commune_actuelle')].index[0])
    except (IndexError, TypeError):
        com_index = 0
    st.selectbox("Commune", communes, index=com_index, key="ui_commune")

    # --- Weights ---
    st.divider()
    with st.expander('Pondérations des critères', expanded=False):
        st.select_slider("Education", [0, 25, 50, 100], value=demo_data.get('poids_education', 100), key="ui_poids_education")
        st.select_slider("Projet Pro", [0, 25, 50, 100], value=demo_data.get('poids_emploi', 100), key="ui_poids_emploi")
        st.select_slider("Logement", [0, 25, 50, 100], value=demo_data.get('poids_logement', 100), key="ui_poids_logement")
        st.select_slider("Inclusion", [0, 25, 50, 100], value=demo_data.get('poids_inclusion', 25), key="ui_poids_inclusion")
        st.select_slider("Mobilité", [0, 25, 50, 100], value=demo_data.get('poids_mobilité', 100), key="ui_poids_mobilité")

    # --- Technical Params ---
    
    with st.expander('Paramètres avancés'):
        st.select_slider("Décote binôme %", [1, 10, 25, 50, 100], value=int(demo_data.get('binome_penalty', 0.5)*100), key="ui_penalite_binome")
        st.select_slider("Population Minimum", [0, 500, 1000, 5000, 10000], value=demo_data.get('pop_min', 1000), key="ui_pop_min")

def display_main_header(name: str):
    """Displays the main header of the input section."""
    if name:
        st.subheader(f"Projet de vie de {name}")
    else:
        st.subheader(f"Projet de vie")

def display_input_tabs(demo_data: dict):
    """Displays the main tabs for user input."""
    app_data = st.session_state.app_data
    
    tab_foyer, tab_edu, tab_emploi, tab_logement, tab_sante, tab_autres, tab_mobilite = st.tabs([
        'Situation familiale', 'Education', 'Projet Professionnel', 'Logement', 'Santé', 'Autres Besoins', 'Mobilité'
    ])

    with tab_foyer:
        col1, col2 = st.columns(2)
        with col1:
            st.radio("Nombre d'adultes", [1, 2], index=demo_data.get('nb_adultes', 1) - 1, horizontal=True, key="ui_nb_adultes")
        with col2:
            st.radio("Nombre d'enfants", [0, 1, 2, 3, 4, 5], index=demo_data.get('nb_enfants', 0), horizontal=True, key="ui_nb_enfants")

    with tab_edu:
        if st.session_state.ui_nb_enfants == 0:
            st.info("Aucun enfant n'a été ajouté dans l'onglet 'Situation familiale'.")
        else:
            col1, col2 = st.columns(2)
            liste_classes = ['Maternelle', 'Elémentaire', 'Collège', 'Lycée']
            for i in range(st.session_state.ui_nb_enfants):
                col = col1 if i % 2 == 0 else col2
                default = demo_data.get('classe_enfants', [])
                try:
                    default_index = liste_classes.index(default[i])
                except (IndexError, ValueError):
                    default_index = 0
                with col:
                    st.selectbox(f'Niveau enfant {i+1}', liste_classes, index=default_index, key=f"ui_classe_enfant_{i}")

    with tab_emploi:
        col1, col2 = st.columns(2)
        codfap_select = app_data['codfap_index'][['Code FAP 341', 'Intitulé FAP 341']].set_index('Code FAP 341')
        codform_select = app_data['codformations_index']
        
        for i in range(st.session_state.ui_nb_adultes):
            with col1:
                default = demo_data.get('codes_metiers', [])
                try:
                    default_val = default[i]
                except (IndexError, TypeError):
                    default_val = []
                st.multiselect(f"Métiers ciblés Adulte {i+1}", codfap_select.index, format_func=lambda x: codfap_select.loc[x].item(), default=default_val, key=f"ui_metiers_adult_{i}")
            with col2:
                default = demo_data.get('codes_formations', [])
                try:
                    default_val = default[i]
                except (IndexError, TypeError):
                    default_val = []
                st.multiselect(f"Formations recherchées Adulte {i+1}", codform_select.index, format_func=lambda x: codform_select.loc[x].item(), default=default_val, key=f"ui_formations_adult_{i}")

    with tab_mobilite:
        options = {25: 'Important (~25km)', 50: 'Assez important (~50km)', 1000: 'Toute la France'}
        default_dist = demo_data.get('loc_distance_km', 50)
        try:
            dist_index = list(options.keys()).index(default_dist)
        except ValueError:
            dist_index = 1
        st.radio('Attachement au lieu de vie actuel :', options.keys(), format_func=options.get, index=dist_index, key="ui_loc_distance_km")

    with tab_logement:
        options_heb = ["Chez l'habitant", 'Location', 'Foyer']
        options_log = ['Location', 'Logement Social']
        try:
            heb_index = options_heb.index(demo_data.get('hebergement'))
        except (ValueError, TypeError):
            heb_index = 1
        try:
            log_index = options_log.index(demo_data.get('logement'))
        except (ValueError, TypeError):
            log_index = 0
        st.radio('Hébergement à court terme', options_heb, index=heb_index, key="ui_hebergement")
        st.radio('Logement à long terme', options_log, index=log_index, key="ui_logement")

    with tab_sante:
        options = ["Aucun", "Hopital", 'Maternité', "Soutien Psychologique & Addictologie"]
        try:
            index = options.index(demo_data.get('sante'))
        except (ValueError, TypeError):
            index = 0
        st.radio('Support médical à proximité', options, index=index, key="ui_besoin_sante")

    with tab_autres:
        if 'ui_besoins_autres' not in st.session_state:
            st.session_state.ui_besoins_autres = demo_data.get('besoins_autres', {})

        st.text("Sélectionnez d'autres besoins:")
        col1, col2 = st.columns(2)
        with col1:
            annuaire_inclusion = app_data['annuaire_inclusion']
            cat = st.selectbox('Catégorie', sorted(set(annuaire_inclusion.categorie)), format_func=lambda x: x.replace('-', ' ').capitalize(), index=2)
            service = st.selectbox('Service', sorted(set(annuaire_inclusion[annuaire_inclusion.categorie == cat].service)), format_func=lambda x: x.replace('-', ' ').capitalize(), index=0)
            if st.button('Ajouter'):
                st.session_state.ui_besoins_autres.setdefault(cat, []).append(service)
                st.session_state.ui_besoins_autres[cat] = sorted(list(set(st.session_state.ui_besoins_autres[cat])))
        with col2:
            st.text('Besoins ajoutés:')
            if not st.session_state.ui_besoins_autres:
                st.info('Aucun')
            else:
                for key, values in st.session_state.ui_besoins_autres.items():
                    st.markdown(f"**{key.replace('-', ' ').capitalize()}**")
                    for value in values:
                        st.markdown(f"&nbsp;&nbsp;&nbsp;- {value.replace('-', ' ').capitalize()}")
            if st.button('Vider', use_container_width=True):
                st.session_state.ui_besoins_autres = {}
                st.rerun()

def create_scoring_config_from_inputs() -> cfg.ScoringConfig:
    """Gathers all user inputs from session_state and creates a ScoringConfig object."""
    app_data = st.session_state.app_data
    
    # Location
    commune_codgeo = app_data['depcom_df'][
        (app_data['depcom_df'].dep_code == st.session_state.ui_departement) & 
        (app_data['depcom_df'].libgeo == st.session_state.ui_commune)
    ].index.item()

    # Education
    classe_enfants = [st.session_state[f"ui_classe_enfant_{i}"] for i in range(st.session_state.ui_nb_enfants)]

    # Employment
    codes_metiers = [st.session_state[f"ui_metiers_adult_{i}"] for i in range(st.session_state.ui_nb_adultes)]
    codes_formations = [st.session_state[f"ui_formations_adult_{i}"] for i in range(st.session_state.ui_nb_adultes)]

    return cfg.ScoringConfig(
        poids_emploi=st.session_state.ui_poids_emploi,
        poids_logement=st.session_state.ui_poids_logement,
        poids_education=st.session_state.ui_poids_education,
        poids_inclusion=st.session_state.ui_poids_inclusion,
        poids_mobilité=st.session_state.ui_poids_mobilité,
        commune_actuelle=commune_codgeo,
        loc_distance_km=st.session_state.ui_loc_distance_km,
        nb_adultes=st.session_state.ui_nb_adultes,
        nb_enfants=st.session_state.ui_nb_enfants,
        hebergement=st.session_state.ui_hebergement,
        logement=st.session_state.ui_logement,
        codes_metiers=codes_metiers,
        codes_formations=codes_formations,
        classe_enfants=classe_enfants,
        besoin_sante=st.session_state.ui_besoin_sante,
        besoins_autres=st.session_state.ui_besoins_autres,
        binome_penalty=st.session_state.ui_penalite_binome / 100,
        pop_min=st.session_state.ui_pop_min
    )

def _result_highlight_callback(index: int):
    """Callback to handle highlighting a result."""
    is_highlighted, highlighted_index = st.session_state.highlighted_result
    
    # If the same button is clicked again, un-highlight it
    if is_highlighted and index == highlighted_index:
        st.session_state.highlighted_result = [False, None]
        st.session_state.center = None # Recenter map
        st.session_state.zoom = None
    else:
        # Highlight the new result
        row = st.session_state.processed_gdf.loc[index]
        st.session_state.highlighted_result = [True, index]
        st.session_state.center = [row.polygon.centroid.y, row.polygon.centroid.x]
        st.session_state.zoom = 11

def display_results_list(name: str):
    """Displays the list of top N results."""
    st.subheader("Meilleurs résultats")
    st.text(f'Voici des localités qui pourraient convenir à {name or "ce projet de vie"}.')
    st.markdown('<style>[class*="st-key-button_top"] .stButton button div {text-align:left; width:100%;}</style>', unsafe_allow_html=True)

    top_n = 5
    df = st.session_state.processed_gdf
    is_highlighted, highlighted_index = st.session_state.highlighted_result

    # Pre-build layers for top results to be shown on map
    for index, row in df.head(top_n).iterrows():
        fg_key = f'Top{index + 1}'
        st.session_state.fg_dict_ref[fg_key] = maps.build_top_result_layer(row, index)

    # Display buttons and details
    for index, row in df.head(top_n).iterrows():
        title = f"Top {index+1} | {row.libgeo}" + (f" (avec {row.libgeo_binome})" if row.binome else "")
        st.button(
            title,
            on_click=_result_highlight_callback,
            args=(index,),
            use_container_width=True,
            key=f'button_top{index+1}',
            type='primary' if is_highlighted and index == highlighted_index else 'secondary'
        )

        if is_highlighted and index == highlighted_index:
            _display_result_details(row)

def _display_result_details(row: pd.Series):
    """Displays the detailed information for a single highlighted result."""
    with st.container(border=True):
        # --- Pitch ---
        pitch = _produce_pitch_markdown(row)
        st.markdown(pitch)

        # --- Radar Chart ---
        cat_scores = row[[col for col in row.index if col.endswith('_cat_score')]]
        cat_scores.rename(lambda x: x.split('_')[0].capitalize(), inplace=True)
        fig = line_polar(theta=cat_scores.index, r=cat_scores.values * 100, line_close=True, range_r=[0, 100])
        fig.update_traces(fill='toself')
        fig.update_layout(margin=dict(l=50, r=50, t=50, b=50))
        st.plotly_chart(fig, use_container_width=True)
        st.caption('Plus le critère s’approche du bord, plus il est attractif.')

        # --- Additional Info ---
        st.divider()
        st.markdown('**Plus d’informations sur cette localité :**')
        with st.expander('Top 10 des métiers recherchés'):
            top_metiers = set(row.be_libfap_top if row.be_libfap_top is not None else [])
            if top_metiers:
                st.markdown("\n".join([f'- {item}' for item in sorted(list(top_metiers))]))
            else:
                st.info("Pas de données disponibles.")
        
        with st.expander('Formations proposées'):
            formations = set(row.noms_formations if row.noms_formations is not None else [])
            if row.binome:
                binome_row = st.session_state.app_data['odis'].loc[row.codgeo_binome]
                formations.update(binome_row.noms_formations if binome_row.noms_formations is not None else [])
            if formations:
                st.markdown("\n".join([f'- {item}' for item in sorted(list(formations))]))
            else:
                st.info("Pas de données disponibles.")
        
        with st.expander("Services d'inclusions proposés"):
            services = st.session_state.app_data['annuaire_inclusion']
            services = services[services.codgeo == row.codgeo]
            if not services.empty:
                for cat, group in services.groupby('categorie'):
                    st.markdown(f"**{cat.replace('-', ' ').capitalize()}**")
                    for item in group.itertuples():
                        if item.service != '-':
                            st.markdown(f"&nbsp;&nbsp;&nbsp;- {item.service.replace('-', ' ').capitalize()}")
            else:
                st.info("Pas de services d'inclusion répertoriés dans cette commune.")

        # --- Links ---
        st.markdown(f"[Page OD&IS]({row.get('url_odis', '#')}) | [Page Wikipedia]({row.get('url_wikipedia', '#')})")

def _produce_pitch_markdown(row: pd.Series) -> str:
    """Generates a summary "pitch" for a result."""
    config = st.session_state.config
    scores_cat = st.session_state.app_data['scores_cat']
    app_data = st.session_state.app_data
    
    pitch_md = []
    population = f"{row['population']:,.0f}".replace(",", " ")
    pitch_md.append(f'**{row["libgeo"]}** ({population} habitants) fait partie de l\'EPCI : **{row["epci_nom"]}**.  ')
    
    score_percent = f"{row['weighted_score'] * 100:.0f}%"
    if row["binome"]:
        pitch_md.append(f'\nEn [binôme](https://www.google.com "Lorsque des communes sont proposées en binômes, c’est qu’ensemble elles correspondent au projet de vie. L’une peut présenter des opportunités d’emplois, l’autre de logements.") avec sa voisine **{row["libgeo_binome"]}**, la correspondance avec le projet est évaluée à **{score_percent}**. ')
    else:
        pitch_md.append(f'\nLa correspondance avec le projet est évaluée à **{score_percent}**. ')

    # --- Top contributing criteria ---
    all_scores = scores_cat['score'].unique()
    crit_scores_cols = [col for col in row.keys() if col in all_scores]
    weighted_scores = {}
    for col in crit_scores_cols:
        cat = scores_cat[scores_cat.score == col]['cat'].iloc[0]
        weight = getattr(config, f'poids_{cat}', 0)
        
        # Apply binome penalty if applicable
        penalty = config.binome_penalty if col + '_binome' in row.index else 0
        
        # Effective score is the max between commune and penalized binome
        score_commune = row[col]
        score_binome = row.get(col + '_binome', 0) * (1 - penalty)
        effective_score = max(score_commune or 0, score_binome or 0)
        
        weighted_scores[col] = effective_score * weight

    sorted_scores = sorted(weighted_scores.items(), key=lambda item: item[1], reverse=True)

    pitch_md.append(f"\nCette localité se distingue par :")
    count = 0
    for score_col, weighted_val in sorted_scores:
        if weighted_val > 0 and count < 5:
            score_details = scores_cat[scores_cat.score == score_col].iloc[0]
            pitch_md.append(f'- {score_details["score_affichage"]}')
            count += 1

    return "\n".join(pitch_md)
