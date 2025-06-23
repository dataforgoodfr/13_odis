{{ config(
    tags = ['gold', 'geographical_references'],
    alias = 'gold_gps_dep_reg'
    )
}}

with departements as 
(
    select 
        concat(geo_coordonnees_lattitude, ',' , geo_coordonnees_longitude) as geo_point,
        nom as commune,
        code as "code_INSEE",
        'Préfecture' as service,
        departement_code as "DEP",
        '' as "REG"
    from {{ ref('geographical_references') }} 
    where préfecture = True
),

regions as 
(
    select 
        concat(geo_coordonnees_lattitude, ',' , geo_coordonnees_longitude) as geo_point,
        nom as commune,
        code as "code_INSEE",
        'Préfecture de région' as service,
        '' as "DEP",
        region_code as "REG"
    from {{ ref('geographical_references') }}
        where préfecture_de_région = True 
)

select * from departements
    UNION ALL
    select * from regions

 

