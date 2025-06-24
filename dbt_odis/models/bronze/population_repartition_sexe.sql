{{ config(
    tags = ['bronze', 'population'],
    alias = 'vw_population_repartition_sexe'
    )
}}

with repartition_sexe as (
    select
        id as id,
        (data::jsonb)->'dimensions'->>'AGE'::text as dimensions_AGE, 
        (data::jsonb)->'dimensions'->>'GEO'::text as dimensions_GEO,
        (data::jsonb)->'dimensions'->>'SEX'::text as dimensions_SEX,
        (data::jsonb)->'dimensions'->>'RP_MEASURE'::text as dimensions_RP_MEASURE,
        (data::jsonb)->'dimensions'->>'TIME_PERIOD'::text as dimensions_TIME_PERIOD,
        (data::jsonb)->'measures'->'OBS_VALUE_NIVEAU'->>'value'::text as measures_OBS_VALUE_NIVEAU_value,
        created_at as created_at

    from {{ source('bronze', 'population_repartition_sexe') }}  
)

select * from repartition_sexe