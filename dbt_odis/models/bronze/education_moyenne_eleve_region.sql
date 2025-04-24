{{ config(
    alias = 'vw_education_moyenne_eleve_region'
    )
}}


with eleve_region as 
(
    select 
        id, 
        (data::jsonb) ->> 'region' as region, 
        (data::jsonb) ->> 'moyenne_eleves_par_classe' as moyenne_eleves_par_classe, 
        created_at

    from {{ source('bronze', 'education_moyenne_eleve_region') }}  
)

select * from eleve_region
