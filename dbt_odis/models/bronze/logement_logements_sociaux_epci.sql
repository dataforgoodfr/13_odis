{{ config(
    alias = 'vw_logement_logements_sociaux_epci'
    )
}}

select 
    {{ dbt_utils.star(from=source('bronze', 'logement_logements_sociaux_epci')) }}
from {{ source('bronze', 'logement_logements_sociaux_epci') }}
