{{ config(
    tags = ['bronze', 'population'],
    alias = 'vw_population_categorie_socio_pro'
) }}

with pop_csp as
(
    select   
        (data::jsonb)->'dimensions'->>'AGE'::text as age,
        (data::jsonb)->'dimensions'->>'GEO'::text as geo,
        (data::jsonb)->'dimensions'->>'PCS'::text as pcs,
        (data::jsonb)->'dimensions'->>'SEX'::text as sex,
        (data::jsonb)->'dimensions'->>'RP_MEASURE'::text as rp_measure,
        (data::jsonb)->'dimensions'->>'TIME_PERIOD'::text as time_period,
        (data::jsonb)->'measures'->'OBS_VALUE_NIVEAU' ->> 'value'::text as measure_value
    from {{ source('bronze', 'population_categorie_socio_pro') }}
)

select * from pop_csp





