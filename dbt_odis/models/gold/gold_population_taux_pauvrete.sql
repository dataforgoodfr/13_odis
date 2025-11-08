{{ config(
    tags = ['gold', 'population', 'taux_pauvrete'],
    alias = 'gold_population_taux_pauvrete'
    )
}}

select
    case
        when admin_level = 'commune' then "CODGEO"
        when admin_level = 'departement' then "CODGEO"
        when admin_level = 'region' then concat('reg', "CODGEO")
        else "CODGEO"
    end as codgeo,    
    "YEAR",
    "TP40",
    "TP50",
    "TP60"
from {{ref("stg_population_taux_pauvrete")}}