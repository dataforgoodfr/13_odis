{{ config(
    alias = 'vw_population_nb_menages'
    )
}}

with menages_pop as 
(
    select   
        (data::jsonb)->'dimensions'->>'AGE'::text as dimensions_AGE,        
        (data::jsonb)->'dimensions'->>'GEO'::text as dimensions_GEO,
        (data::jsonb)->'dimensions'->>'NOC'::text as dimensions_NOC,  
        (data::jsonb)->'dimensions'->>'OCS'::text as dimensions_OCS, 
        (data::jsonb)->'dimensions'->>'FREQ'::text as dimensions_FREQ, 
        (data::jsonb)->'dimensions'->>'COUPLE'::text as dimensions_COUPLE, 
        (data::jsonb)->'dimensions'->>'RP_MEASURE'::text as dimensions_RP_MEASURE,
        (data::jsonb)->'dimensions'->>'TIME_PERIOD'::text as dimensions_TIME_PERIOD,
        (data::jsonb)->'dimensions'->>'CIVIL_STATUS'::text as dimensions_CIVIL_STATUS,
        (data::jsonb)->'measures'->'OBS_VALUE_NIVEAU'->>'value'::text as measures_OBS_VALUE_NIVEAU_value   

    from {{ source('bronze', 'population_nb_menages') }}  
    
)

select * from menages_pop


