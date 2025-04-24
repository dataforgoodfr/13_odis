{{ config(
    alias = 'vw_education_moyenne_eleve_departement'
    )
}}


with eleve_departement as 
(
    select 
        id, 
        (data::jsonb) ->> 'departement' as departement, 
        (data::jsonb) ->> 'moyenne_eleves_par_classe' as moyenne_eleves_par_classe, 
        created_at

    from {{ source('bronze', 'education_moyenne_eleve_departement') }}  
)

select * from eleve_departement
