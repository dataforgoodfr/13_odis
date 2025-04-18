{{ config(
    alias = 'vw_education_moyenne_eleve_commune'
    )
}}


with eleve_commune as 
(
    select 
        id as id, 
        {{ generate_flatten_json(
            model_name = {{ source('bronze', 'education_moyenne_eleve_commune') }},
            json_column = 'data'
        ) }}
        created_at as created_at

    from  {{ source('bronze', 'education_moyenne_eleve_commune') }}
)

select * from eleve_commune

