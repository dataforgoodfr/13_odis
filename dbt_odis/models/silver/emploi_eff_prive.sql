{{ config(
    alias='silver_emploi_eff_secteur_prive_gds_secteurs'
    )
}}

select 
    {{ get}}
from {{ ref("emploi_eff_prive_gds_secteurs") }}