import streamlit as st
# import streamlit.components.v1 as components
import folium as flm
from streamlit_folium import st_folium
print('#####rerun#######')


fg_results = flm.FeatureGroup(name="Results")
CENTER_START = [44.833161, -0.621072]
ZOOM_START = 12
m = flm.Map(location=CENTER_START, zoom_start=ZOOM_START)
st_folium(
    m,
    #feature_group_to_add=fg_results,#fg_dict[fg],
    width=400,
    height=300,
    key="odis_scored_map",
    # layer_control=flm.LayerControl(collapsed=False)
)