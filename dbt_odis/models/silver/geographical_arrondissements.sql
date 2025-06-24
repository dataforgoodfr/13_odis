{{ config(
    tags = ['silver', 'geographical_references'],
    alias = 'silver_geographical_arrondissements'
    )
}}

select 
    id, 
    nom,
    case
        when nom like '%Paris%' then 
            (select code from {{ ref('geographical_references_communes') }} where nom = 'Paris')
        when nom like '%Lyon%' then 
            (select code from {{ ref('geographical_references_communes') }} where nom = 'Lyon')
        when nom like '%Marseille%' then 
            (select code from {{ ref('geographical_references_communes') }} where nom = 'Marseille')
        when nom like '%Bordeaux%' then 
            (select code from {{ ref('geographical_references_communes') }} where nom = 'Bordeaux')
        when nom like '%Toulouse%' then 
            (select code from {{ ref('geographical_references_communes') }} where nom = 'Toulouse')
        when nom like '%Nice%' then 
            (select code from {{ ref('geographical_references_communes') }} where nom = 'Nice')
        when nom like '%Nantes%' then 
            (select code from {{ ref('geographical_references_communes') }} where nom = 'Nantes')
        when nom like '%Strasbourg%' then 
            (select code from {{ ref('geographical_references_communes') }} where nom = 'Strasbourg')
        when nom like '%Montpellier%' then 
            (select code from {{ ref('geographical_references_communes') }} where nom = 'Montpellier')
        when nom like '%Rennes%' then 
            (select code from {{ ref('geographical_references_communes') }} where nom = 'Rennes')
        when nom like '%Lille%' then 
            (select code from {{ ref('geographical_references_communes') }} where nom = 'Lille')
        else null
    end as code_aggregation,
    code_geo, 
    code_region, 
    code_departement,
    code_postal,
    cast(population as integer) as population,
    created_at
from {{ ref('geographical_references_arrondissements') }} 
where char

order by code_geo