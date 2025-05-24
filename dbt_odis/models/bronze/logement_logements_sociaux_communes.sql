{{ config(
    tags = ['bronze', 'logement_social'],
    alias = 'vw_logement_social_logements_sociaux_communes'
    )
}}

select 
    {{ dbt_utils.star(from=source('bronze', 'logement_social_logements_sociaux_communes')) }}
from {{ source('bronze', 'logement_social_logements_sociaux_communes') }}
