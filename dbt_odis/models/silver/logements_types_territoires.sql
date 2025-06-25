{{ config(
    tags = ['silver', 'logement'],
    alias = 'silver_logements_types_territoires'
    )
}}

with appartements as (
    select 
        dimensions_GEO,
        dimensions_TIME_PERIOD,
        max(case when dimensions_ocs = '_T' then measures_OBS_VALUE_NIVEAU_value end) as total_appartements,
        max(case when dimensions_ocs = 'DW_MAIN' then measures_OBS_VALUE_NIVEAU_value end) as rp_appartements,
        max(case when dimensions_ocs = 'DW_SEC_DW_OCC' then measures_OBS_VALUE_NIVEAU_value end) as rsecocc_appartements,
        max(case when dimensions_ocs = 'DW_VAC' then measures_OBS_VALUE_NIVEAU_value end) as lvac_appartements
    from {{ ref('logements_appartement') }}
    group by dimensions_GEO, dimensions_TIME_PERIOD  
),

maisons as 
(
    select 
        dimensions_GEO,
        dimensions_TIME_PERIOD,
        max(case when dimensions_ocs = '_T' then measures_OBS_VALUE_NIVEAU_value end) as total_maisons,
        max(case when dimensions_ocs = 'DW_MAIN' then measures_OBS_VALUE_NIVEAU_value end) as rp_maisons,
        max(case when dimensions_ocs = 'DW_SEC_DW_OCC' then measures_OBS_VALUE_NIVEAU_value end) as rsecocc_maisons,
        max(case when dimensions_ocs = 'DW_VAC' then measures_OBS_VALUE_NIVEAU_value end) as lvac_maisons
    from {{ ref('logements_maison') }} 
    group by dimensions_GEO, dimensions_TIME_PERIOD
),

pieces as 
(
    select 
        dimensions_GEO,
        dimensions_TIME_PERIOD,
        dimensions_RP_MEASURE,
        max(case when dimensions_tdw = '_T' then measures_OBS_VALUE_NIVEAU_value end) as total_rp_pieces,
        max(case when dimensions_tdw = '1' then measures_OBS_VALUE_NIVEAU_value end) as maisons_rp_pieces,
        max(case when dimensions_tdw = '2' then measures_OBS_VALUE_NIVEAU_value end) as appartements_rp_pieces,
        max(case when dimensions_tdw = '3T6' then measures_OBS_VALUE_NIVEAU_value end) as autres_rp_pieces
    from {{ ref('logements_pieces') }} 
    group by dimensions_GEO, dimensions_TIME_PERIOD, dimensions_RP_MEASURE
),

total_logements as 
(
    select 
        dimensions_GEO,
        dimensions_TIME_PERIOD,
        dimensions_RP_MEASURE,
        max(case when dimensions_ocs = '_T' and dimensions_tdw = '_T' then measures_OBS_VALUE_NIVEAU_value end) as total_tous_logements,
        max(case when dimensions_ocs = 'DW_MAIN' and dimensions_tdw = '_T' then measures_OBS_VALUE_NIVEAU_value end) as rp_tous_logements,
        max(case when dimensions_ocs = 'DW_SEC_DW_OCC' and dimensions_tdw = '_T' then measures_OBS_VALUE_NIVEAU_value end) as rsecocc_tous_logements,
        max(case when dimensions_ocs = 'DW_VAC' and dimensions_tdw = '_T' then measures_OBS_VALUE_NIVEAU_value end) as lvac_tous_logements
    from {{ ref('logements_total') }} 
    group by dimensions_GEO, dimensions_TIME_PERIOD, dimensions_RP_MEASURE
),

joined as (
    select 
        tot.dimensions_GEO as geo,
        tot.dimensions_TIME_PERIOD as time_period,
        tot.dimensions_RP_MEASURE as habitations,
        tot.total_tous_logements,
        tot.rp_tous_logements,
        tot.rsecocc_tous_logements,
        tot.lvac_tous_logements,
        maisons.total_maisons,
        maisons.rp_maisons,
        maisons.rsecocc_maisons,
        maisons.lvac_maisons,
        appts.total_appartements,
        appts.rp_appartements,
        appts.rsecocc_appartements,
        appts.lvac_appartements,
        pieces.dimensions_RP_MEASURE as pieces_habitations,
        pieces.total_rp_pieces,
        pieces.maisons_rp_pieces,
        pieces.appartements_rp_pieces,
        pieces.autres_rp_pieces
    from total_logements tot
        left join maisons maisons on maisons.dimensions_GEO = tot.dimensions_GEO and maisons.dimensions_TIME_PERIOD = tot.dimensions_TIME_PERIOD
        left join appartements appts on appts.dimensions_GEO = tot.dimensions_GEO and appts.dimensions_TIME_PERIOD = tot.dimensions_TIME_PERIOD
        left join pieces pieces on pieces.dimensions_GEO = tot.dimensions_GEO and pieces.dimensions_TIME_PERIOD = tot.dimensions_TIME_PERIOD
)

select 
    geo,
    time_period,
    habitations,
    cast(total_tous_logements as float) as total_tous_logements,
    cast(rp_tous_logements as float) as rp_tous_logements,
    cast(rsecocc_tous_logements as float) as rsecocc_tous_logements,
    cast(lvac_tous_logements as float) as lvac_tous_logements,
    cast(total_maisons as float) as total_maisons,
    cast(rp_maisons as float) as rp_maisons,
    cast(rsecocc_maisons as float) as rsecocc_maisons,
    cast(lvac_maisons as float) as lvac_maisons,
    cast(total_appartements as float) as total_appartements,
    cast(rp_appartements as float) as rp_appartements,
    cast(rsecocc_appartements as float) as rsecocc_appartements,
    cast(lvac_appartements as float) as lvac_appartements,
    pieces_habitations,
    cast(total_rp_pieces as float) as total_rp_pieces,
    cast(maisons_rp_pieces as float) as maisons_rp_pieces,
    cast(appartements_rp_pieces as float) as appartements_rp_pieces,
    cast(autres_rp_pieces as float) as autres_rp_pieces,
    case
        when geo like '%COM%' then 'commune'
        when geo like '%DEP%' then 'departement'
        when geo like '%REG%' then 'region'
    end as type_geo,
    case
        when geo like '%COM%' then right(geo, 5)
        when geo like '%DEP%' then replace(right(geo, 3), '-', '')
        when geo like '%REG%' then right(geo, 2)
    end as code_geo
from joined





 

