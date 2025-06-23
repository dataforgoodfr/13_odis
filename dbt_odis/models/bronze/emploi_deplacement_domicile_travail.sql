{{ config(
    tags = ['bronze', 'emploi'],
    alias='vw_emploi_deplacement_domicile_travail'
    )
}}

select 
    {{ dbt_utils.star(from=source('bronze', 'emploi_deplacement_domicile_travail_ds_rp_navettes_princ_data')) }}
from {{ source('bronze', 'emploi_deplacement_domicile_travail_ds_rp_navettes_princ_data') }}