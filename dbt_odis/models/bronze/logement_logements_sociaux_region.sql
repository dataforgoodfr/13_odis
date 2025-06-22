{{ config(
    tags = ['bronze', 'logement_social'],
    alias = 'vw_logement_logements_sociaux_region'
    )
}}

select
    "REG" as reg,
    "LIBREG" as libreg,
    {{ dbt_utils.star(
        from=source('bronze', 'logement_social_logements_sociaux_region'),
        except=["REG", "LIBREG"] 
    ) }}
from {{ source('bronze', 'logement_social_logements_sociaux_region') }}
