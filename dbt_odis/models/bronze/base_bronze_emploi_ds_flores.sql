{{ config(
    tags = ['bronze', 'emploi_ds_flores'],
    alias = 'vw_emploi_ds_flores'
    )
}}


with ds_flores as 
(
    select 
        id,
        (obs.value::jsonb)->'dimensions'->>'GEO'::text as dimensions_GEO,
        (obs.value)->'dimensions'->>'ECONOMIC_SPHERE'::text as dimentions_economic_sphere,
        (obs.value)->'dimensions'->>'FLORES_MEASURE'::text as dimensions_flores_measure,
        (obs.value)->'dimensions'->>'LEGAL_FORM_WITH_PUBLIC'::text as dimensions_legal_form,
        (obs.value)->'dimensions'->>'TIME_PERIOD'::text as dimensions_TIME_PERIOD,
        (obs.value)->'measures'->'OBS_VALUE_NIVEAU'->>'value'::text as measures_OBS_VALUE_NIVEAU_value,
        created_at
    from {{ source('bronze', 'emploi_ds_flores') }} , jsonb_array_elements((data::jsonb)->'observations') as obs
)

select * from ds_flores