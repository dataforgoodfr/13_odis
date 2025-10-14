{{ config(
    tags = ['bronze', 'emploi_ds_flores'],
    alias = 'vw_emploi_ds_flores'
    )
}}


with ds_flores as 
(
    select 
    
        (obs.value::jsonb)->'dimensions'->>'GEO'::text as dimensions_GEO,
        (obs.value::jsonb)->'dimensions'->>'ECONOMIC_SPHERE'::text as dimensions_economic_sphere,
        (obs.value::jsonb)->'dimensions'->>'FLORES_MEASURE'::text as dimensions_flores_measure,
        (obs.value::jsonb)->'dimensions'->>'LEGAL_FORM_WITH_PUBLIC'::text as dimensions_legal_form,
        (obs.value::jsonb)->'dimensions'->>'TIME_PERIOD'::text as dimensions_TIME_PERIOD,
        (obs.value::jsonb)->'measures'->'OBS_VALUE_NIVEAU'->>'value'::text as measures_OBS_VALUE_NIVEAU_value,
        created_at
    from {{ source('bronze', 'emploi_ds_flores') }} , jsonb_array_elements((data::jsonb)->'observations') as obs
),

ds_flores_wunique_id as(
    select
        {{ dbt_utils.generate_surrogate_key(['dimensions_GEO', 
                                        'dimensions_legal_form', 
                                        'dimensions_TIME_PERIOD',
                                        'dimensions_economic_sphere',
                                        'dimensions_flores_measure']) }} as id,
        dimensions_GEO, 
        dimensions_economic_sphere, 
        dimensions_flores_measure,
        dimensions_legal_form,
        cast(dimensions_TIME_PERIOD as int),
        cast(measures_OBS_VALUE_NIVEAU_value as float)
    from ds_flores
)

  select * from ds_flores_wunique_id