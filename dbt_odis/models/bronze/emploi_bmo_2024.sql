{{ config(
    alias='vw_emploi_bmo_2024'
    )
}}

select 
    {{ dbt_utils.star(from=source('bronze', 'emploi_bmo_2024_file')) }}
from {{ source('bronze', 'emploi_bmo_2024_file') }}