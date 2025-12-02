{{ config(
    tags=['gold', 'population'],
    alias='vw_gold_population_densite'
) }}

with densite as (
    select
        codegeo,
        "year",
        population,
        superficie,
        cast(round(population / nullif(superficie * 1e-2, 0), 0) as int) as densite -- densite au km carré, mais superficie exprimée en m2
    from {{ ref('silver_population_population_superficie') }}
    where codegeo_type in ('COM', 'REG', 'DEP') -- niveaux requis d'après la documentation Jaccueille
    and ocs = '_T' -- population totale
)

select * from densite