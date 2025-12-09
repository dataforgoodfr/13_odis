{{ config(
    tags = ['bronze', 'population'],
    alias = 'vw_population_by_age_gender'
) }}


with people as 
(
    select 
        id,
        (data::jsonb)->'dimensions'->>'GEO'::text as geo,
        (data::jsonb)->'dimensions'->>'SEX'::text as sex,
        (data::jsonb)->'dimensions'->>'AGE'::text as age,
        (data::jsonb)->'dimensions'->>'RP_MEASURE'::text as rp_measure,
        (data::jsonb)->'dimensions'->>'TIME_PERIOD'::text as time_period,
        (data::jsonb)->'measures'->'OBS_VALUE_NIVEAU'->>'value'::text as measure_value,
        created_at
    from {{ source('bronze', 'population_by_age_gender') }} 
)

select * from people
