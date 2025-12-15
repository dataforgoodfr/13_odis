{{ config(
    tags = ['bronze', 'presentation','elu'],
    alias = 'vw_presentation_elections_regionales'
    )
}}

select
  {{ dbt_utils.star(from=source('bronze', 'presentation_elections_regionales'), 
    except=["reg_code"]) }},
  case
    when dep_code = 'ZA' then '01'
    when dep_code = 'ZB' then '02'
    when dep_code = 'ZC' then '03'
    when dep_code = 'ZD' then '04'
    when dep_code = '27' then '28'
    else lpad(trunc(reg_code::numeric)::text, 2, '0')
  end as reg_code
from {{ source('bronze', 'presentation_elections_regionales') }} 
