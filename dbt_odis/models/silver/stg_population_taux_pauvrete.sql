{{ config(
    tags = ['silver', 'population', 'taux_pauvrete'],
    alias='vw_population_taux_pauvrete_stg',
    materialized='view'
) }}

with cleaned_tp as (

    select        
        "CODGEO",
        year as "YEAR",
        {{cast_tp_to_float("TP4021",  "TP40")}}, 
        {{cast_tp_to_float("TP5021",  "TP50")}}, 
        {{cast_tp_to_float("TP6021",  "TP60")}}, 
        admin_level,
        case when "TP4021" = 's' then true else false end as tp40_is_secret_stat,  -- s stands for couvert par le secret statistique
        case when "TP4021" = 'nd' then true else false end as tp40_is_not_available, --nd stands for donnée non disponible
        case when "TP5021" = 's' then true else false end as tp50_is_secret_stat, -- other codes might exist, not found in this table
        case when "TP5021" = 'nd' then true else false end as tp50_is_not_available,
        case when "TP6021" = 's' then true else false end as tp60_is_secret_stat,
        case when "TP6021" = 'nd' then true else false end as tp60_is_not_available    
    
    from {{ref("population_taux_pauvrete")}}
    where "CODGEO" not like '751%' --we remove arrondissements, only keep the whole city for Paris, Marseilles and Lyon
        and "CODGEO" not like '693%'
        and "CODGEO" not like '132%'
)

select * from cleaned_tp