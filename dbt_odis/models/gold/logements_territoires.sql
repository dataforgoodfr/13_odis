{{ config(
    tags = ['gold', 'logement'],
    alias = 'gold_logements_territoires'
    )
}}

with logement_ratio as (
    select
        id,
        geo,
        time_period,
        maisons,
        maisons_rp,
        maisons - maisons_rp as nb_maisons_secondaires,
        appartements,
        appartements_rp,
        appartements - appartements_rp as nb_appartements_secondaires,
        pieces as pieces,
        appartements_rp + maisons_rp as total_logements_principaux,
        (appartements - appartements_rp + maisons - maisons_rp) as total_logements_secondaires,
        total_logements,
        type_geo,
        code_geo
    from {{ ref('logements_types_territoires') }}
)

select
    case
        when type_geo = 'commune' then code_geo
        when type_geo = 'departement' then code_geo
        when type_geo = 'region' then concat('reg', code_geo)
        else code_geo
    end as codgeo,
    extract(year from cast(time_period || '-01-01' as date)) as "YEAR",
    total_logements as "LOG",
    total_logements_principaux as "RP",
    total_logements_secondaires as "RSECOCC",
    total_logements_secondaires as "LOGVAC",
    maisons as "MAISON",
    appartements as "APPART",
    maisons_rp as "RPMAISON",
    appartements_rp as "RPAPPART", 
    pieces / nullif((maisons + appartements), 0) as "NB_MOY_PIECE",
    total_logements_principaux as "MEN",
    total_logements_principaux * (pieces / nullif((maisons + appartements), 0)) as "NBPI_RP"
    {# ROUND(cast(nb_maisons_secondaires as numeric) / nullif(maisons, 0) * 100, 2) as taux_maisons_vacantes,
    ROUND(cast(nb_appartements_secondaires as numeric) / NULLIF(appartements, 0) * 100, 2) as taux_appartements_vacants,
    ROUND(cast(total_logements_secondaires as numeric) / NULLIF(total_logements, 0) * 100, 2) as taux_total_logements_vacants,
    type_geo,
    code_geo #}
from logement_ratio
    where time_period ~ '^\d{4}$'
