{{ config(
    tags = ['bronze', 'population', 'taux_pauvrete'],
    alias='vw_population_taux_pauvrete'
    )
}}

select 
    cast("CODGEO" as text) as "CODGEO", "TP4021", "TP5021", "TP6021", 'commune' as admin_level, 2021 as year
from {{ source('bronze', 'population_taux_pauvrete_communes_filo2021_disp_pauvres_com') }}

union

select 
    cast("CODGEO" as text) as "CODGEO", "TP4021", "TP5021", "TP6021", 'departement' as admin_level, 2021 as year
from {{ source('bronze', 'population_taux_pauvrete_supra_filo2021_disp_pauvres_dep') }}

union

select 
    cast("CODGEO" as text) as "CODGEO", "TP4021", "TP5021", "TP6021", 'region' as admin_level, 2021 as year
from {{ source('bronze', 'population_taux_pauvrete_supra_filo2021_disp_pauvres_reg') }}