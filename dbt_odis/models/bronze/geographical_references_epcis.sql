{{ config(
    tags = ['bronze', 'geographical', 'epci'],
    alias='vw_geographical_references_epcis'
    )
}}

select 
    {{ dbt_utils.star(from=source('bronze', 'geographical_references_epcis')) }}
from {{ source('bronze', 'geographical_references_epcis') }}