{{ config(
    tags = ['bronze', 'emploi'],
    alias='vw_emploi_salaire_median_filo2021'
    )
}}


select 
    "CODGEO" as "codgeo",
    2021 as YEAR,
    "Q221" as Mediane_Annuelle
from {{ source('bronze', 'emploi_salaire_median_filo2021_disp_com') }}