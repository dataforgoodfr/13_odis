{{ config(
    alias = 'silver_com_dep_reg'
    )
}}

with communes as 
(
    select 
        code as codgeo, 
        intitule as libgeo
    from {{ source('bronze', 'vw_geographical_references_communes') }}  
),

departements as 
(
    select 
        code as coddep
    from {{ source('bronze', 'vw_geographical_references_departements') }} 
),

regions as 
(
    select 
        code as codreg
    from {{ source('bronze', 'vw_geographical_references_regions') }} 
)

select 
    com.codgeo,
    reg.codreg,
    dep.coddep,
    com.libgeo

from regions reg 
    left join departements dep 
        on reg.codgeo = substring(dep.coddep, 1, 2)
    left join communes com 
        on dep.coddep = substring(com.codgeo, 1, 2)

