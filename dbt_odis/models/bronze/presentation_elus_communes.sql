{{ config(
    tags = ['bronze', 'presentation'],
    alias = 'vw_presentation_elus_communes'
    )
}}

select
  {{ dbt_utils.star(from=source('bronze', 'presentation_elus_communes')) }}
from {{ source('bronze', 'presentation_elus_communes') }} 
