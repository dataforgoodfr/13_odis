{{ config(
    tags = ['bronze', 'presentation'],
    alias = 'vw_presentation_population_communes'
    )
}}


with population_communes as 
(
    select 
        (data::jsonb)->'measures' -> 'OBS_VALUE_NIVEAU' ->> 'value' as value, 
        (data::jsonb)->'dimensions' ->> 'AGE' as "AGE", 
        (data::jsonb)->'dimensions' ->> 'GEO' as "GEO",
        (data::jsonb)->'dimensions' ->> 'SEX' as "SEX",
        (data::jsonb)->'dimensions' ->> 'RP_MEASURE' as "RP_MEASURE",
        (data::jsonb)->'dimensions' ->> 'TIME_PERIOD' as "TIME_PERIOD",
        created_at
    from {{ source('bronze', 'presentation_population_communes') }} 
)

select * from population_communes
