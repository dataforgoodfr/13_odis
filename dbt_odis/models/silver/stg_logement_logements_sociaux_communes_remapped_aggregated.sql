{{ config(
    alias = 'vw_stg_logement_logements_sociaux_communes_remapped_aggregated',
    tags = ['silver', 'logement_social'],
    materialized = 'view'
) }}

with

-- Communes staging data
base as (
    select * from {{ ref('stg_logement_logements_sociaux_communes') }}
),

-- Arrondissement-to-commune mappings
arr_mapping as (
    select 
        code_geo as arrondissement_code,
        code_aggregation as commune_code
    from {{ ref('geographical_arrondissements') }}
),

-- Map arrondissement codes to commune codes (only where needed)
remapped as (
    select
        coalesce(m.commune_code, b.codgeo) as codgeo_mapped,
        b.libcom,
        b.dep,
        b.reg,
        b.year,
        b.nb_ls,
        b.tx_vac,
        b.tx_vac3,
        b.tx_mob,
        b.nb_loues,
        b.nb_vacants,
        b.nb_vides,
        b.nb_asso,
        b.nb_occup_finan,
        b.nb_occup_temp,
        b.nb_ls_en_qpv,
        b.densite,
        -- flag if this row was an arrondissement originally
        case when m.commune_code is not null then 'arrondissement' else b.type_geo end as type_geo
    from base b
    left join arr_mapping m
        on b.codgeo = m.arrondissement_code
),

-- Aggregate by new code + year
aggregated as (
    select
        codgeo_mapped as codgeo,
        max(libcom) as libcom,  -- conservative choice, in case of duplicates
        max(dep) as dep,
        max(reg) as reg,
        year,
        max(type_geo) as type_geo,
        sum(nb_ls) as nb_ls,
        avg(tx_vac) as tx_vac,
        avg(tx_vac3) as tx_vac3,
        avg(tx_mob) as tx_mob,
        sum(nb_loues) as nb_loues,
        sum(nb_vacants) as nb_vacants,
        sum(nb_vides) as nb_vides,
        sum(nb_asso) as nb_asso,
        sum(nb_occup_finan) as nb_occup_finan,
        sum(nb_occup_temp) as nb_occup_temp,
        sum(nb_ls_en_qpv) as nb_ls_en_qpv,
        avg(densite) as densite
    from remapped
    group by codgeo_mapped, year
)

select * from aggregated
