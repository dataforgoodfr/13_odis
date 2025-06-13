{{ config(
    tags = ['silver', 'geographical_references'],
    alias = 'silver_geographical_references'
    )
}}

select 
    code,
    nom,
    geo_type,
    geo_coordonnees_lattitude,
    geo_coordonnees_longitude,
    departement_nom,
    departement_code,
    case 
        when code in (select chef_lieu from {{ ref('geographical_references_departements') }}) then True
        else false
    end as préfecture,
    region_nom,
    region_code,
    case 
        when code in (select chef_lieu from {{ ref('geographical_references_regions') }}) then True
        else false
    end as préfecture_de_région,
    cast (population as integer) as population,
    case 
        when cast(population as integer) < 100 then 'Moins de 100'
        when cast(population as integer) < 200 then 'Entre 100 et 200'
        when cast(population as integer) < 350 then 'Entre 200 et 350'
        when cast(population as integer) < 500 then 'Entre 350 et 500'
        when cast(population as integer) < 750 then 'Entre 500 et 750'
        when cast(population as integer) < 1000 then 'Entre 750 et 1000'
        when cast(population as integer) < 2000 then 'Entre 1000 et 2000'
        when cast(population as integer) < 5000 then 'Entre 2000 et 5000'
        when cast(population as integer) < 10000 then 'Entre 5000 et 10000'
        when cast(population as integer) < 50000 then 'Entre 10000 et 50000'
        when cast(population as integer) < 100000 then 'Entre 50000 et 100000'
        when cast(population as integer) > 100000 then 'Supérieur à 100000'
    end as tranche_de_population
from {{ ref('geographical_references_communes') }} 
