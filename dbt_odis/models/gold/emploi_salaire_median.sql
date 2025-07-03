{{ config(
    tags = ['gold', 'emploi'],
    alias='emploi_salaire_median'
    )
}}

select
    codgeo,
    year as "YEAR",
    Mediane_Annuelle as "Mediane_Annuelle"
from {{ ref('emploi_salaire_median_filo2021') }}