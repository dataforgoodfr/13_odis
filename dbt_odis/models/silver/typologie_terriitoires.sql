{{ config(
    alias = 'silver_typologies_territoires'
    )
}}

select 
    code as CodGeo,
    nom as Libellé,
    case 
        when cast(population as INTEGER) < 100 then 'Moins de 100'
        when cast(population as INTEGER) < 200 then 'Entre 100 et 200'
        when cast(population as INTEGER) < 350 then 'Entre 200 et 350'
        when cast(population as INTEGER) < 500 then 'Entre 350 et 500'
        when cast(population as INTEGER) < 750 then 'Entre 500 et 750'
        when cast(population as INTEGER) < 1000 then 'Entre 750 et 1000'
        when cast(population as INTEGER) < 2000 then 'Entre 1000 et 2000'
        when cast(population as INTEGER) < 5000 then 'Entre 2000 et 5000'
        when cast(population as INTEGER) < 10000 then 'Entre 5000 et 10000'
        when cast(population as INTEGER) < 50000 then 'Entre 10000 et 50000'
        when cast(population as INTEGER) < 100000 then 'Entre 50000 et 100000'
        when cast(population as INTEGER) > 100000 then 'Supérieur à 100000'
    end as Tranche_de_population,
    population
from {{ ref('geographical_references_communes') }}  


