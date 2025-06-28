{{ config(
    tags = ['bronze', 'geographical'],
    alias='vw_geographical_intercommunalites'
    )
}}

select 
    {{ dbt_utils.star(from=source('bronze', 'geographical_references_intercommunalites')) }}
from {{ source('bronze', 'geographical_references_intercommunalites') }}