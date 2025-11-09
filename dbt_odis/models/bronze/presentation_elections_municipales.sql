{{ config(
    tags = ['bronze', 'presentation'],
    alias = 'vw_presentation_elections_municipales'
    )
}}

select
  {{ dbt_utils.star(from=source('bronze', 'presentation_elections_municipales')) }} 
from {{ source('bronze', 'presentation_elections_municipales') }} 
