{{ config(
    tags = ['bronze', 'presentation'],
    alias = 'vw_presentation_elections_departementales'
    )
}}

select
   {{ dbt_utils.star(from=source('bronze', 'presentation_elections_departementales')) }} 
from {{ source('bronze', 'presentation_elections_departementales') }} 
