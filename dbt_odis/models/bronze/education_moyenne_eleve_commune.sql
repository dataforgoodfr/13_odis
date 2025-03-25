{{ config(
    alias = 'vw_education_moyenne_eleve_commune'
    )
}}


with eleve_commune as 
(
    select 
        id as id, 
        json_value(data, '$.commune') as commune, 
        json_value(data, '$.moyenne_eleves_par_classe') as moyenne_eleves_par_classe, 
        created_at as created_at

    from {{ source('bronze', 'education_moyenne_eleve_commune') }}  
)

select * from eleve_commune
