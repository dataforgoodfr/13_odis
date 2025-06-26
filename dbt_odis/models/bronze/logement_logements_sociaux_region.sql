{{ config(
    tags = ['bronze', 'logement_social'],
    alias = 'vw_logement_logements_sociaux_region'
    )
}}

select
    case
        when "REG" = 1 then '01'
        when "REG" = 2 then '02'
        when "REG" = 3 then '03'
        when "REG" = 4 then '04'
        when "REG" = 5 then '05'
        when "REG" = 6 then '06'
        when "REG" = 7 then '07'
        when "REG" = 8 then '08'
        when "REG" = 9 then '09'
        else cast("REG" as text)
    end as reg,
    "LIBREG" as libreg,
    {{ dbt_utils.star(
        from=source('bronze', 'logement_social_logements_sociaux_region'),
        except=["REG", "LIBREG"] 
    ) }}
from {{ source('bronze', 'logement_social_logements_sociaux_region') }}
