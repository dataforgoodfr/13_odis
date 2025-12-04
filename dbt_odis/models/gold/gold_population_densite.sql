{{ config(
    tags=['gold', 'population'],
    alias='vw_gold_population_densite'
) }}

with densite as (
    select
        -- ajout d'un prefix pour les regions car on pourrait les confondre avec les departements
        case
            when codegeo_type = 'REG' then concat('reg', codegeo) else codegeo end
        as codegeo,
        "year",
        population,
        superficie,
        -- densite au km2, mais superficie exprimee en m2
        cast(round(population / nullif(superficie * 1e-2, 0), 0) as int) as densite
    from {{ ref('stg_population_population_superficie') }}
    where "year" = '2022'
)

select * from densite