{{ config(
    alias = 'vw_presentation_page_epci'
    )
}}

select 
    {{ dbt_utils.star(from=source('bronze', 'presentation_page_epci')) }}
from {{ source('bronze', 'presentation_page_epci') }} 
