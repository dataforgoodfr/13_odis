{{ config(
    tags = ['bronze', 'population'],
    alias = 'vw_population_population_superficie'
    )
}}

with pop_superficie as
(
    select
        id,
        {# {{ extract_json_keys_recursive(
            table_ref = source('bronze', 'population_population_superficie'),
            json_column = 'data'
        ) }}, #}
        (data::jsonb)->'dimensions'->>'GEO'::text as geo,
        (data::jsonb)->'dimensions'->>'OCS'::text as ocs,
        (data::jsonb)->'dimensions'->>'FREQ'::text as freq,
        (data::jsonb)->'dimensions'->>'RP_MEASURE'::text as rp_measure,
        (data::jsonb)->'dimensions'->>'TIME_PERIOD'::text as time_period,
        (data::jsonb)->'measures'->'OBS_VALUE_NIVEAU'->>'value'::text as measure_value

    from {{ source('bronze', 'population_population_superficie') }}

)

select * from pop_superficie