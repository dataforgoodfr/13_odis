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
)

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
    left join pieces pieces on pieces.geo = tot.geo;






 

