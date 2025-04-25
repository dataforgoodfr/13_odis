{{ config(
    alias = 'vw_presentation_page_elus_communes'
    )
}}

select 
    {{ dbt_utils.star(from=source('bronze', 'presentation_page_elus_communes')) }}
from {{ source('bronze', 'presentation_page_elus_communes') }} 
