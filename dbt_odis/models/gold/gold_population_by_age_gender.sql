{{ config(
    tags=['gold', 'population'],
    alias='vw_gold_population_by_age_gender'    
) }}

with code_geo_final as (
    select
        case
            when typegeo = 'departement' then SPLIT_PART(codegeo, '-', 3)
            when typegeo = 'region' then concat('reg', SPLIT_PART(codegeo, '-', 3))
        end as codegeo,
        "year",
        pop,
        poph,
        H0014,
        H1529,
        H3044,
        H4559,
        H6074,
        H7589,
        H90P,
        popf,
        F0014,
        F1529,
        F3044,
        F4559,
        F6074,
        F7589,
        F90P
    from {{ ref('silver_population_by_age_gender') }}
    where typegeo != 'france_entiere'
)

select * from code_geo_final