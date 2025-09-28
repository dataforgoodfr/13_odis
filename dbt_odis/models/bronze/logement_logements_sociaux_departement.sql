{{ config(
    tags = ['bronze', 'logement_social'],
    alias = 'vw_logement_logements_sociaux_departement'
    )
}}

select 
    "DEP" as dep,
    "LIBDEP" as libdep,
    {{ dbt_utils.star(
        from=source('bronze', 'logement_social_logements_sociaux_departement'),
        except=["DEP", "LIBDEP"]
    ) }}
from {{ source('bronze', 'logement_social_logements_sociaux_departement') }}
