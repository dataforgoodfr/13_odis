{{ config(
    tags = ['gold', 'homepage'],
    alias = 'gold_homepage_accueil'
) }}

WITH

stacked_accueil as (
    select
        code_officiel_commune,
        sha.type,
        sum(places) as places,
        count(id) as compte
    from {{ ref('silver_homepage_accueil')}} sha
    group by code_officiel_commune, sha.type
),
type_pivot as (
    select
        code_officiel_commune as codgeo,
        LEFT(code_officiel_commune, 2) as code_departement,
        max(case when sa.type = 'CAES' then places end) as caes,
        max(case when sa.type = 'HUDA' then places end) as huda,
        max(case when sa.type = 'PRAHDA' then places end) as prahda,
        max(case when sa.type = 'DISPOSITIF REFUGIES' then places end) as disp_refu,
        max(case when sa.type = 'CADA' then places end) as cada,
        max(case when sa.type = 'CPH' then places end) as cph
    from stacked_accueil sa
    group by code_officiel_commune
    order by code_officiel_commune
),
departement_agg as (
    select
        code_departement as codgeo,
        sum(caes) as caes,
        sum(huda) as huda,
        sum(prahda) as prahda,
        sum(disp_refu) as disp_refu,
        sum(cada) as cada,
        sum(cph) as cph
    from type_pivot
    group by code_departement
    order by code_departement
),
region_agg as (
    select
        concat('reg', cdr."CODREG") as codgeo,
        sum(tp.caes) as caes,
        sum(tp.huda) as huda,
        sum(tp.prahda) as prahda,
        sum(tp.disp_refu) as disp_refu,
        sum(tp.cada) as cada,
        sum(tp.cph) as cph
    from type_pivot tp
    join {{ ref('com_dep_reg') }} cdr
        on tp.codgeo = cdr."CODGEO"
    group by cdr."CODREG"
    order by cdr."CODREG"
)
select codgeo, cada, huda, prahda, cph, caes, disp_refu from type_pivot
    union all
select * from departement_agg
    union all
select * from region_agg
    order by codgeo