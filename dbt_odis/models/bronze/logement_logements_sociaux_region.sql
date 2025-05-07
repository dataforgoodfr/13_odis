{{ config(
    alias = 'vw_logement_logements_sociaux_region'
    )
}}

select 
    {{ dbt_utils.star(from=source('bronze', 'logement_logements_sociaux_region')) }}
from {{ source('bronze', 'logement_logements_sociaux_region') }}
