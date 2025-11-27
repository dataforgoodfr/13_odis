{{ config(
    tags = ['bronze', 'presentation','elu'],
    alias = 'vw_presentation_elus_communes'
    )
}}

select
  {{ dbt_utils.star(from=source('bronze', 'presentation_elus_communes'), 
    except=["dep_code","reg_code"]) }},
  lpad(trunc(reg_code::numeric)::text, 2, '0')  as reg_code,
  lpad(trunc(dep_code::numeric)::text, 2, '0') as dep_code
from {{ source('bronze', 'presentation_elus_communes') }}
