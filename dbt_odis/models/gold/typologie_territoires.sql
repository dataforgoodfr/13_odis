{{ config(
    tags = ['gold', 'geographical_references'],
    alias = 'gold_typologies_territoires'
    )
}}

select 
    code as CodGeo,
    nom as Libellé,
    tranche_de_population,
    population
from {{ ref('geographical_references') }}  


