{{ config(
tags = ['bronze', 'emploi'],
alias='vw_emploi_salaire_median_filo2021_disp_com'
)
}}

select
{{ dbt_utils.star(from=source('bronze', 'emploi_salaire_median_filo2021_disp_com')) }}
from {{ source('bronze', 'emploi_salaire_median_filo2021_disp_com') }}