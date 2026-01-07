{{ config(
    tags = ['bronze', 'presentation','elu'],
    alias = 'vw_presentation_elections_municipales'
    )
}}

select
  {{ dbt_utils.star(from=source('bronze', 'presentation_elections_municipales'), 
    except=["code_officiel_commune"]) }},
  case 
    when length(code_officiel_commune) > 5 
    then left(code_officiel_commune, 2) || substring(code_officiel_commune FROM 4)
    else code_officiel_commune
  end as code_officiel_commune
from {{ source('bronze', 'presentation_elections_municipales') }}
