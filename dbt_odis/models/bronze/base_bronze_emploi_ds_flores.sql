{{ config(
    tags = ['bronze', 'emploi_ds_flores'],
    alias = 'vw_emploi_ds_flores'
    )
}}


with ds_flores as 
(
    select 
        id,
        {# {{ extract_json_keys_recursive(
            table_ref = source('bronze', 'emploi_ds_flores'),
            json_column = 'data'
        ) }}, #}
        (data::jsonb)->'dimensions'->>'GEO'::text as dimensions_GEO,
        (data::jsonb)->'dimensions'->>'ECONOMIC_SPHERE'::text as dimentions_economic_sphere,
        (data::jsonb)->'dimensions'->>'FLORES_MEASURE'::text as dimensions_flores_measure,
        (data::jsonb)->'dimensions'->>'LEGAL_FORM_WITH_PUBLIC'::text as dimensions_legal_form,
        (data::jsonb)->'dimensions'->>'TIME_PERIOD'::text as dimensions_TIME_PERIOD,
        (data::jsonb)->'measures'->'OBS_VALUE_NIVEAU'->>'value'::text as measures_OBS_VALUE_NIVEAU_value,
        created_at
    from {{ source('bronze', 'emploi_ds_flores') }} 
)

select * from ds_flores