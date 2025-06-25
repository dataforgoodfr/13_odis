{{ config(
    tags = ['bronze', 'logement_social'],
    alias = 'vw_logement_logements_sociaux_departement'
    )
}}

select 
    "DEP" as dep,
    "Unnamed: 1" as libdep,
    {{ dbt_utils.star(
        from=source('bronze', 'logement_social_logements_sociaux_departement'),
        except=["DEP", "Unnamed: 1"]
    ) }}
from {{ source('bronze', 'logement_social_logements_sociaux_departement') }}
