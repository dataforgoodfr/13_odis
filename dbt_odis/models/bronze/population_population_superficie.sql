{{ config(
    tags = ['bronze', 'population'],
    alias = 'vw_population_population_superficie'
    )
}}


with population_superficie as (
    select
            id as id,
            (data::jsonb)->'dimensions'->>'GEO'::text as dimensions_GEO,
            (data::jsonb)->'dimensions'->>'OCS'::text as dimensions_OCS,
            (data::jsonb)->'dimensions'->>'FREQ'::text as dimensions_FREQ,
            (data::jsonb)->'dimensions'->>'RP_MEASURE'::text as dimensions_RP_MEASURE,
            (data::jsonb)->'dimensions'->>'TIME_PERIOD'::text as dimensions_TIME_PERIOD,
            (data::jsonb)->'measures'->'OBS_VALUE_NIVEAU'->>'value'::text as measures_OBS_VALUE_NIVEAU_value,
            created_at as created_at
    from {{  source('bronze', 'population_population_superficie')}}
)

select * from population_superficie

