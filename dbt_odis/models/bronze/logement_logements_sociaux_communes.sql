{{ config(
    tags = ['bronze', 'logement_social'],
    alias = 'vw_logement_logements_sociaux_communes'
    )
}}

select
    "DEPCOM_ARM" as codgeo,
    "LIBCOM" as libcom,
    "DEP" as dep,
    "LIBCOM_DEP" as libcom_dep,
    "REG" as reg, 
    {{ dbt_utils.star(
        from=source('bronze', 'logement_social_logements_sociaux_communes'),
        except=["DEPCOM_ARM", "LIBCOM", "DEP", "LIBCOM_DEP", "REG"]
    ) }}
from {{ source('bronze', 'logement_social_logements_sociaux_communes') }}
