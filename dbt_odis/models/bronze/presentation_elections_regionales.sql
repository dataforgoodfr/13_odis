{{ config(
    tags = ['bronze', 'presentation'],
    alias = 'vw_presentation_elections_regionales'
    )
}}

select
  {{ dbt_utils.star(from=source('bronze', 'presentation_elections_regionales')) }} 
from {{ source('bronze', 'presentation_elections_regionales') }} 
