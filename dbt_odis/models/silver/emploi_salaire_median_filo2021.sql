{{ config(
    tags = ['silver', 'emploi'],
    alias='emploi_salaire_median_filo2021'
    )
}}

select 
    "CODGEO" as codgeo,
    2021 as year,
    "Q221" as Mediane_Annuelle
from {{ source('bronze', 'emploi_salaire_median_filo2021_disp_com') }}