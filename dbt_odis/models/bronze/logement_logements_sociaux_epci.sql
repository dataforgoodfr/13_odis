{{ config(
    tags=['bronze', 'logement_social'],
    alias='vw_logement_logements_sociaux_epci'
) }}

select
    "EPCI_DEP" as epci_dep,
    "LIBEPCI" as libepci,
    "DEP" as dep,
    {{ dbt_utils.star(
        from=source('bronze', 'logement_social_logements_sociaux_epci'),
        except=["EPCI_DEP", "LIBEPCI", "DEP"]
        ) }}
from {{ source('bronze', 'logement_social_logements_sociaux_epci') }}
