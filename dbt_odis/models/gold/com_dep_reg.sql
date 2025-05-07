{{ config(
    alias = 'gold_com_dep_reg'
    )
}}

select 
    code as CODGEO,
    region_code as CODREG,
    departement_code as CODDEP,
    nom as LIBGEO
from {{ ref('geographical_references') }} 

