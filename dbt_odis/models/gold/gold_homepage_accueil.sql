{{ config(
    tags = ['gold', 'homepage'],
    alias = 'gold_homepage_accueil'
) }}

WITH

stacked_accueil as (
    SELECT
        code_officiel_commune,
        sha.type,
        sum(places) as places,
        count(id) as compte
    from {{ ref('silver_homepage_accueil')}} sha
    GROUP BY code_officiel_commune, sha.type
),
type_pivot as (
    select
        code_officiel_commune as codegeo,
        LEFT(code_officiel_commune, 2) as code_region,
        max(case when sa.type = 'CAES' then places end) as caes,
        max(case when sa.type = 'HUDA' then places end) as huda,
        max(case when sa.type = 'PRAHDA' then places end) as prahda,
        max(case when sa.type = 'DISPOSITIF REFUGIES' then places end) as disp_refu,
        max(case when sa.type = 'CADA' then places end) as cada,
        max(case when sa.type = 'CPH' then places end) as cph
    FROM stacked_accueil sa
    group by code_officiel_commune
    ORDER BY code_officiel_commune
),
region_sum as (
    SELECT
        code_region as codegeo,
        sum(caes) as caes,
        sum(huda) as huda,
        sum(prahda) as prahda,
        sum(disp_refu) as disp_refu,
        sum(cada) as cada,
        sum(cph) as cph
    from type_pivot
    group by code_region
    ORDER BY code_region
)

SELECT codegeo, cada, huda, prahda, cph, caes, disp_refu
FROM type_pivot

UNION

SELECT codegeo, cada, huda, prahda, cph, caes, disp_refu
FROM region_sum

order by codegeo