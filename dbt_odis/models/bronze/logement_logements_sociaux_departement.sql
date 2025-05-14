{{ config(
    tags = ['bronze', 'logement_social'],
    alias = 'vw_logement_logements_sociaux_departement'
    )
}}

select 
    {{ dbt_utils.star(from=source('bronze', 'logement_logements_sociaux_departement')) }}
from {{ source('bronze', 'logement_logements_sociaux_departement') }}
