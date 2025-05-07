{{ config(
    alias='vw_emploi_eff_secteur_prive_gds_secteurs'
    )
}}

select 
    {{ dbt_utils.star(from=source('bronze', 'emploi_eff_secteur_prive_gds_secteurs')) }}
from {{ source('bronze', 'emploi_eff_secteur_prive_gds_secteurs') }}