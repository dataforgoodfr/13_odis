{{ config(
    tags = ['silver', 'logement'],
    alias = 'silver_logements_types_territoires'
    )
}}

with apt_ as 
(
    select 
        distinct measures_OBS_VALUE_NIVEAU_value,
        dimensions_GEO
    from {{ ref('logements_appartement') }}  
),

apt_rp as 
(
    select 
        distinct measures_OBS_VALUE_NIVEAU_value,
        dimensions_GEO
    from {{ ref('logements_appartement_et_residences_principales') }} 
),

maison_ as 
(
    select 
        distinct measures_OBS_VALUE_NIVEAU_value,
        dimensions_GEO
    from {{ ref('logements_maison') }} 
),

maison_rp as 
(
    select 
        distinct measures_OBS_VALUE_NIVEAU_value,
        dimensions_GEO
    from {{ ref('logements_maison_et_residences_principales') }} 
),

pieces as 
(
    select 
        distinct measures_OBS_VALUE_NIVEAU_value,
        dimensions_GEO

    from {{ ref('logements_pieces') }} 
),

total as 
(
    select 
        distinct *
    from {{ ref('logements_total') }} 
),

joined as (
    select 
        tot.id,
        tot.dimensions_GEO as geo,
        tot.dimensions_TIME_PERIOD as time_period,
        maison.measures_OBS_VALUE_NIVEAU_value as maisons,
        maison_rp.measures_OBS_VALUE_NIVEAU_value as maisons_rp,
        apt.measures_OBS_VALUE_NIVEAU_value as appartements,
        apt_rp.measures_OBS_VALUE_NIVEAU_value as appartements_rp,
        pieces.measures_OBS_VALUE_NIVEAU_value as pieces,
        tot.measures_OBS_VALUE_NIVEAU_value as total_logements
    from total tot
        left join maison_ maison on maison.dimensions_GEO = tot.dimensions_GEO
        left join maison_rp maison_rp on maison_rp.dimensions_GEO = tot.dimensions_GEO
        left join apt_ apt on apt.dimensions_GEO = tot.dimensions_GEO
        left join apt_rp apt_rp on apt_rp.dimensions_GEO = tot.dimensions_GEO
        left join pieces pieces on pieces.dimensions_GEO = tot.dimensions_GEO
)

select 
    *,
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





 

