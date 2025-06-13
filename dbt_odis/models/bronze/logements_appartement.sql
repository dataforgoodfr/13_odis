{{ config(
    tags = ['bronze', 'logement'],
    alias = 'vw_logements_appartement'
    )
}}


with appartement as 
(
    select 
        id,
        {# {{ extract_json_keys_recursive(
            table_ref = source('bronze', 'logement_logements_appartement_et_residences_principales'),
            json_column = 'data'
        ) }}, #}
        (data::jsonb)->'dimensions'->>'GEO'::text as dimensions_GEO,
        (data::jsonb)->'dimensions'->>'NOR'::text as dimensions_NOR,
        (data::jsonb)->'dimensions'->>'OCS'::text as dimensions_OCS,
        (data::jsonb)->'dimensions'->>'TDW'::text as dimensions_TDW,
        (data::jsonb)->'dimensions'->>'TOH'::text as dimensions_TOH,
        (data::jsonb)->'dimensions'->>'TSH'::text as dimensions_TSH,
        (data::jsonb)->'dimensions'->>'CARS'::text as dimensions_CARS,
        (data::jsonb)->'dimensions'->>'L_STAY'::text as dimensions_L_STAY,
        (data::jsonb)->'dimensions'->>'CARPARK'::text as dimensions_CARPARK,
        (data::jsonb)->'dimensions'->>'BUILD_END'::text as dimensions_BUILD_END,
        (data::jsonb)->'dimensions'->>'RP_MEASURE'::text as dimensions_RP_MEASURE,
        (data::jsonb)->'dimensions'->>'TIME_PERIOD'::text as dimensions_TIME_PERIOD,
        (data::jsonb)->'measures'->'OBS_VALUE_NIVEAU'->>'value'::text as measures_OBS_VALUE_NIVEAU_value,
        created_at
    from {{ source('bronze', 'logement_logements_appartement') }} 
)

select * from appartement
