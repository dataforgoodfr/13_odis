{{ config(
    alias = 'vw_logement_logements_sociaux_communes'
    )
}}

select 
    {{ dbt_utils.star(from=source('bronze', 'logement_logements_sociaux_communes')) }}
from {{ source('bronze', 'logement_logements_sociaux_communes') }}
