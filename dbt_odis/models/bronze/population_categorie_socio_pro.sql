{{ config(
    alias = 'vw_population_categorie_socio_pro'
    )
}}


with departments as 
(
    select 
        (data::jsonb)->'dimensions'->>'AGE'::text as dimensions_AGE,        
        (data::jsonb)->'dimensions'->>'GEO'::text as dimensions_GEO,
        (data::jsonb)->'dimensions'->>'PCS'::text as dimensions_PCS,
        (data::jsonb)->'dimensions'->>'SEX'::text as dimensions_SEX,
        (data::jsonb)->'dimensions'->>'RP_MEASURE'::text as dimensions_RP_MEASURE,
        (data::jsonb)->'dimensions'->>'TIME_PERIOD'::text as dimensions_TIME_PERIOD 

    from {{ source('bronze', 'population_categorie_socio_pro') }}  
    
)

select * from departments





