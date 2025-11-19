{{ config(
    tags = ['bronze', 'presentation'],
    alias = 'vw_presentation_population_communes'
) }}

with population_communes as 
(
    select 
        obs->'measures'->'OBS_VALUE_NIVEAU'->>'value' as value, 
        obs->'dimensions'->>'AGE' as "AGE", 
        obs->'dimensions'->>'GEO' as "GEO",
        obs->'dimensions'->>'SEX' as "SEX",
        obs->'dimensions'->>'RP_MEASURE' as "RP_MEASURE",
        obs->'dimensions'->>'TIME_PERIOD' as "TIME_PERIOD",
        created_at
    from {{ source('bronze', 'presentation_population_communes') }},
    jsonb_array_elements(data->'observations') as obs
)

select * from population_communes
