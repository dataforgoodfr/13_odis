{{ config(
    tags = ['bronze', 'logement_social'],
    alias = 'vw_logement_social_logements_sociaux_epci'
    )
}}

select 
    {{ dbt_utils.star(from=source('bronze', 'logement_social_logements_sociaux_epci')) }}
from {{ source('bronze', 'logement_social_logements_sociaux_epci') }}
