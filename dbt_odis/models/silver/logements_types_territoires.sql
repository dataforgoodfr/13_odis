{{ config(
    alias = 'silver_logements_types_territoires'
    )
}}

with apt_ as 
(
    select 
        distinct value,
        geo
    from {{ ref('logements_appartement') }}  
),

apt_rp as 
(
    select 
        distinct value,
        geo
    from {{ ref('logements_appartement_et_residences_principales') }} 
),

maison_ as 
(
    select 
        distinct value,
        geo
    from {{ ref('logements_maison') }} 
),

maison_rp as 
(
    select 
        distinct value,
        geo
    from {{ ref('logements_maison_et_residences_principales') }} 
),

pieces as 
(
    select 
        distinct value,
        geo

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
        tot.geo,
        tot.time_period,
        maison.value as maisons,
        maison_rp.value as maisons_rp,
        apt.value as appartements,
        apt_rp.value as appartements_rp,
        pieces.value as pieces,
        tot.value as total_logements
    from total tot
        left join maison_ maison on maison.geo = tot.geo
        left join maison_rp maison_rp on maison_rp.geo = tot.geo
        left join apt_ apt on apt.geo = tot.geo
        left join apt_rp apt_rp on apt_rp.geo = tot.geo
        left join pieces pieces on pieces.geo = tot.geo
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





 

