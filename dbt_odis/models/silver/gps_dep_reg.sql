{{ config(
    alias = 'silver_gps_dep_reg'
    )
}}

with communes as 
(
    select 
        concat(geo_coordonnees_lattitude, ',' , geo_coordonnees_longitude) as geo_point,
        nom as commune,
        code 
    from {{ ref('geographical_references_communes') }}  
),

departements as 
(
    select 
        code as DEP,
        chef_lieu as code_INSEE,
        'Préfecture' as service,
        '' as REG
    from {{ ref('geographical_references_departements') }} 
),

regions as 
(
    select 
        code as REG,
        chef_lieu as code_INSEE,
        'Préfecture de région' as service,
        '' as DEP
    from {{ ref('geographical_references_regions') }} 
),

depcom as 
(
    select 
        com.geo_point,
        dep.service,
        dep.code_INSEE,
        com.commune,
        dep.DEP,
        dep.REG
    from departements dep
        left join communes com 
        on dep.code_INSEE = com.code
),

regcom as 
(
    select 
        com.geo_point,
        reg.service,
        reg.code_INSEE,
        com.commune,
        reg.DEP,
        reg.REG
    from regions reg
        left join communes com 
        on reg.code_INSEE = com.code

)

select * from depcom
    UNION ALL
    select * from regcom

 

