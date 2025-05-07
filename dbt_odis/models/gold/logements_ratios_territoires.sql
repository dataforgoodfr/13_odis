{{ config(
    alias = 'gold_logements_ratios_territoires'
    )
}}



with logement_ratio as (
    select
        id,
        geo,
        time_period,
        cast(cast(maisons as float) as integer) as maisons,
        cast(cast(maisons_rp as float) as integer) as maisons_rp,
        cast(cast(maisons as float) as integer) - cast(cast(maisons_rp as float) as integer) as nb_maisons_secondaires,
        cast(cast(appartements as float) as integer) as appartements,
        cast(cast(appartements_rp as float) as integer) as appartements_rp,
        cast(cast(appartements as float) as integer) - cast(cast(appartements_rp as float) as integer) as nb_appartements_secondaires,
        cast(cast(pieces as float) as integer) as pieces,
        (cast(cast(appartements as float) as integer) - cast(cast(appartements_rp as float) as integer) + cast(cast(maisons as float) as integer) - cast(cast(maisons_rp as float) as integer)) as total_logements_secondaires,
        cast(cast(total_logements as float) as integer) as total_logements,
        type_geo,
        code_geo
    from {{ ref('logements_types_territoires') }}
)

select
    id,
    geo,
    time_period,
    maisons,
    maisons_rp, 
    nb_maisons_secondaires,
    ROUND(cast(nb_maisons_secondaires as numeric) / NULLIF(maisons, 0) * 100, 2) as taux_maisons_vacantes,
    appartements,
    appartements_rp,
    nb_appartements_secondaires, 
    ROUND(cast(nb_appartements_secondaires as numeric) / NULLIF(appartements, 0) * 100, 2) as taux_appartements_vacants,
    pieces,
    total_logements_secondaires,
    total_logements,
    ROUND(cast(total_logements_secondaires as numeric) / NULLIF(total_logements, 0) * 100, 2) as taux_total_logements_vacants,
    type_geo,
    code_geo
from logement_ratio
