{{ config(
    tags = ['bronze', 'population'],
    alias = 'vw_population_by_age_gender'
    )
}}


with people as 
(
    select 
        id,
        {# {{ extract_json_keys_recursive(
            table_ref = source('bronze', 'population_by_age_gender'),
            json_column = 'data'
        ) }}, #}
        (data::jsonb)->'dimensions'->>'GEO'::text as dimensions_GEO,
        (data::jsonb)->'dimensions'->>'SEX'::text as dimensions_SEX,
        (data::jsonb)->'dimensions'->>'AGE'::text as dimensions_AGE,
        (data::jsonb)->'dimensions'->>'EP_MEASURE'::text as dimensions_EP_MEASURE,
        (data::jsonb)->'dimensions'->>'TIME_PERIOD'::text as dimensions_TIME_PERIOD,
        (data::jsonb)->'measures'->'OBS_VALUE_NIVEAU'->>'value'::text as measures_OBS_VALUE_NIVEAU_value,
        created_at
    from {{ source('bronze', 'population_by_age_gender') }} 
)

select * from people
