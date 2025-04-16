{{ config(
    alias = 'vw_education_moyenne_eleve_region'
    )
}}


with eleve_region as 
(
    select 
        id as id, 
        json_value(data, '$.region_academique') as region_academique, 
        json_value(data, '$.moyenne_eleves_par_classe') as moyenne_eleves_par_classe, 
        created_at as created_at

    from {{ source('bronze', 'education_moyenne_eleve_region') }}  
)

select * from eleve_region
