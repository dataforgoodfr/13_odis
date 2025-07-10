{{ config(
    tags = ['gold', 'logement'],
    alias = 'gold_logements_territoires'
    )
}}

with logements as (
    select
        geo,
        time_period,
        total_tous_logements,
        rp_tous_logements,
        rsecocc_tous_logements,
        lvac_tous_logements,
        total_maisons,
        rp_maisons,
        total_appartements,
        rp_appartements,
        total_rp_pieces,
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
    total_tous_logements as "LOG",
    rp_tous_logements as "RP",
    rsecocc_tous_logements as "RSECOCC",
    lvac_tous_logements as "LOGVAC",
    total_maisons as "MAISON",
    total_appartements as "APPART",
    rp_maisons as "RPMAISON",
    rp_appartements as "RPAPPART",
    round((total_rp_pieces / nullif(rp_tous_logements, 0))::numeric, 5) as "NB_MOY_PIECE",
    rp_tous_logements as "MEN",
    total_rp_pieces as "NBPI_RP"
from logements
    where time_period ~ '^\d{4}$'
