# /home/jacques/odis/13_odis/eda/streamlit/maps.py
import streamlit as st
import folium as flm
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import mapping
from branca.colormap import linear
from folium.plugins import FastMarkerCluster

import config as cfg

def get_map_zoom(distance_km: int) -> int:
    """Returns a map zoom level based on a search distance."""
    if distance_km <= 10: return 11
    if distance_km <= 25: return 10
    if distance_km <= 50: return 9
    if distance_km <= 100: return 8
    return 7

def create_base_map(center: list, zoom: int):
    """Creates the base Folium map."""
    if center is None: center = cfg.DEFAULT_MAP_CENTER
    if zoom is None: zoom = get_map_zoom(st.session_state.config.loc_distance_km)
    return flm.Map(location=center, zoom_start=zoom, tiles="cartodbpositron")

def build_scores_layer(df: pd.DataFrame) -> tuple:
    """Builds the FeatureGroup for all scored communes, colored by score."""
    fg = flm.FeatureGroup(name="Scores")
    
    score_dict = df.set_index("codgeo")["weighted_score"]
    colormap = linear.YlGn_09.scale(score_dict.min(), score_dict.max())

    # Add current commune in blue
    current_geo_df = st.session_state.selected_geo
    current_geo_df_serializable = current_geo_df[['libgeo', 'polygon']].copy()
    current_geo_df_serializable.set_geometry('polygon', inplace=True)

    flm.GeoJson(
        current_geo_df_serializable,
        style_function=lambda x: {"fillColor": 'blue', "fillOpacity": 0.5, "stroke": True, "color": "blue"},
        tooltip=current_geo_df['libgeo'].iloc[0]
    ).add_to(fg)

    # Add all scored communes
    df_serializable = df[['codgeo', 'libgeo', 'weighted_score', 'polygon']].copy()
    df_serializable.set_geometry('polygon', inplace=True)

    flm.GeoJson(
        df_serializable,
        style_function=lambda feature: {
            "fillColor": colormap(score_dict.get(feature["properties"]["codgeo"])),
            "color": "grey",
            "weight": 1,
            "fillOpacity": 0.7,
        },
        tooltip=flm.GeoJsonTooltip(fields=['libgeo', 'weighted_score'], aliases=['Commune:', 'Score:'], fmt=['', '{:.0%}']),
    ).add_to(fg)

    return fg, colormap

def build_top_result_layer(row: pd.Series, index: int) -> flm.FeatureGroup:
    """Builds a FeatureGroup to highlight a single top result (commune + binome)."""
    fg = flm.FeatureGroup(name=f"Top{index + 1}")
    
    # Main commune
    flm.GeoJson(
        mapping(row.polygon),
        style_function=lambda x: {"color": "red", "fillOpacity": 0, "weight": 3}
    ).add_to(fg)

    # Binome commune (if it exists)
    if row.binome and row.polygon_binome:
        flm.GeoJson(
            mapping(row.polygon_binome),
            style_function=lambda x: {"color": "red", "fillOpacity": 0, "weight": 2, "dashArray": "5, 5"}
        ).add_to(fg)
        
    return fg

def build_legend(items_list: list) -> str:
    """Builds an HTML legend for the map."""
    leaflet_colors = {
        "red": "#D63E2A", "blue": "#38A9DC", "green": "#72B026", "purple": "#5B396B",
        "orange": "#F69730", "grey": "#A3A3A3"
    }
    
    legend_html = """
        <div style='position: absolute; z-index: 9999; background-color: rgba(255, 255, 255, 0.8);
                    border-radius: 6px; padding: 10px; font-size: 12px; right: 10px; top: 10px;'>
            <ul style='list-style: none; margin: 0; padding: 0;'>
                <li><span style='display: inline-block; width:100px; background: linear-gradient(90deg, #ffffcc 0%, #006837 100%); 
                    border: 1px solid grey;'>&nbsp;</span> Score</li>
    """
    for item in items_list:
        color = leaflet_colors.get(item['color'], 'grey')
        legend_html += f"""
            <li><i class='fa fa-{item['icon']}' style='color:{color}; width: 20px; text-align: center;'></i> {item['text']}</li>
        """
    legend_html += "</ul></div>"
    return legend_html

def _build_generic_points_layer(df: gpd.GeoDataFrame, icon: str, color: str, tooltip_cols: list) -> flm.FeatureGroup:
    """Generic helper to build a FastMarkerCluster layer."""
    df = df.copy()
    df = df[~df.is_empty & df.notna()]
    if df.empty:
        return flm.FeatureGroup()

    df['lat'] = df.geometry.y
    df['lon'] = df.geometry.x
    
    locations = df[['lat', 'lon'] + tooltip_cols].values.tolist()
    
    # Create a JS callback for the markers
    popup_str = " + '<br>' + ".join([f"'{col}: ' + row[{i+2}]" for i, col in enumerate(tooltip_cols)])
    callback = f"""
    function (row) {{
        var icon, marker;
        icon = L.AwesomeMarkers.icon({{icon: '{icon}', markerColor: '{color}', prefix: 'fa'}});
        marker = L.marker(new L.LatLng(row[0], row[1]));
        marker.setIcon(icon);
        marker.bindPopup('<b>' + {popup_str} + '</b>');
        return marker;
    }};
    """
    return FastMarkerCluster(locations, callback=callback)

def build_ecoles_layer(annuaire_ecoles: gpd.GeoDataFrame, target_codgeos: set, config: cfg.ScoringConfig) -> flm.FeatureGroup:
    """Builds the map layer for schools."""
    fg = flm.FeatureGroup(name="Établissements Scolaires")
    
    filtered = annuaire_ecoles[annuaire_ecoles['code_commune'].isin(target_codgeos)].copy()
    if not config.classe_enfants:
        return fg # No kids, no schools to show
        
    niveaux_map = {
        'Maternelle': (filtered.ecole_maternelle > 0),
        'Elémentaire': (filtered.ecole_elementaire > 0),
        'Collège': (filtered.type_etablissement == 'Collège'),
        'Lycée': (filtered.type_etablissement == 'Lycée')
    }
    
    mask = pd.Series(False, index=filtered.index)
    for niveau in config.classe_enfants:
        if niveau in niveaux_map:
            mask |= niveaux_map[niveau]
            
    filtered = filtered[mask]
    
    cluster = _build_generic_points_layer(filtered, icon='pencil', color='green', tooltip_cols=['nom_etablissement', 'type_etablissement'])
    cluster.add_to(fg)
    return fg

def build_sante_layer(annuaire_sante: gpd.GeoDataFrame, target_codgeos: set, config: cfg.ScoringConfig) -> flm.FeatureGroup:
    """Builds the map layer for health facilities."""
    fg = flm.FeatureGroup(name="Établissements de Santé")
    filtered = annuaire_sante[annuaire_sante['codgeo'].isin(target_codgeos)].copy()

    mask = pd.Series(False, index=filtered.index)
    if config.besoin_sante == 'Maternité':
        mask = filtered.maternite == True
    elif config.besoin_sante == "Hopital":
        mask = filtered.Categorie.isin(['355', '362', '101', '106'])
    elif config.besoin_sante == "Soutien Psychologique & Addictologie":
        mask = filtered.Categorie.isin(['156', '292', '425', '412', '366', '415', '430', '444'])
    
    if not mask.any():
        return fg

    cluster = _build_generic_points_layer(filtered[mask], icon='plus', color='blue', tooltip_cols=['RaisonSociale', 'LibelleCategorieAgregat'])
    cluster.add_to(fg)
    return fg

def build_services_layer(annuaire_inclusion: gpd.GeoDataFrame, target_codgeos: set, config: cfg.ScoringConfig) -> flm.FeatureGroup:
    """Builds the map layer for inclusion services."""
    fg = flm.FeatureGroup(name="Services d'inclusion")
    
    if not config.besoins_autres:
        return fg
        
    filtered = annuaire_inclusion[annuaire_inclusion['codgeo'].isin(target_codgeos)].copy()
    mask = filtered.categorie.isin(config.besoins_autres.keys())
    
    if not mask.any():
        return fg

    cluster = _build_generic_points_layer(filtered[mask], icon='heart', color='purple', tooltip_cols=['nom', 'categorie', 'service'])
    cluster.add_to(fg)
    return fg
