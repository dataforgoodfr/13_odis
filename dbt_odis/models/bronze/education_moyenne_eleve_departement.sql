{{ config(
    alias = 'vw_education_moyenne_eleve_departement'
    )
}}


with eleve_departement as 
(
    select 
        id as id, 
        json_value(data, '$.departement') as departement, 
        json_value(data, '$.moyenne_eleves_par_classe') as moyenne_eleves_par_classe, 
        created_at as created_at

    from {{ source('bronze', 'education_moyenne_eleve_departement') }}  
)

select * from eleve_departement
