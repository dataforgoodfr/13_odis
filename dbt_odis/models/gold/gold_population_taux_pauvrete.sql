{{ config(
    tags = ['gold', 'population', 'taux_pauvrete'],
    alias = 'gold_population_taux_pauvrete'
    )
}}

select
    "CODGEO" as codgeo,
    "YEAR",
    "TP40",
    "TP50",
    "TP60",
    admin_level
from {{ref("stg_population_taux_pauvrete")}}