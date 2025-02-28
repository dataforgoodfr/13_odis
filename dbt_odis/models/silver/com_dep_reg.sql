{{ config(
    alias = 'silver_com_dep_reg'
    )
}}

with communes as 
(
    select 
        code as codgeo, 
        intitule as libgeo
    from {{ ref('geographical_references_communes') }}  
),

departements as 
(
    select 
        code as coddep
    from {{ ref('geographical_references_departements') }} 
),

regions as 
(
    select 
        code as codreg
    from {{ ref('geographical_references_regions') }} 
)

select 
    com.codgeo,
    reg.codreg,
    dep.coddep,
    com.libgeo

from communes com 
    left join regions reg
        on reg.codreg = substring(com.codgeo, 1, 2)
    left join departements dep 
        on dep.coddep = substring(com.codgeo, 1, 2)

