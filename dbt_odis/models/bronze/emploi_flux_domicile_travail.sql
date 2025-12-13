{{ config(
    tags = ['bronze', 'emploi'],
    alias='vw_emploi_flux_domicile_travail'
    )
}}

select 
    {{ dbt_utils.star(from=source('bronze', 'emploi_base_flux_mobilite_domicile_lieu_travail_2022')) }}
from {{ source('bronze', 'emploi_base_flux_mobilite_domicile_lieu_travail_2022') }}