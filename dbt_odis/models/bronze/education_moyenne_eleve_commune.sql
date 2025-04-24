{{ config(
    alias = 'vw_education_moyenne_eleve_commune'
    )
}}


with eleve_commune as 
(
    select 
        id, 
        {# {{ generate_flatten_json_education(
            model_name = source('bronze', 'education_moyenne_eleve_commune'),
            json_column = 'data'
        ) }}, #}
        (data::jsonb) ->> 'commune' as commune, 
        (data::jsonb) ->> 'moyenne_eleves_par_classe' as moyenne_eleves_par_classe, 
        created_at

    from  {{ source('bronze', 'education_moyenne_eleve_commune') }}
)

select * from eleve_commune

