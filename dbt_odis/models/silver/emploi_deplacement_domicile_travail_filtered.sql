{{ config(
    tags = ['silver', 'emploi'],
    alias='vw_emploi_deplacement_domicile_travail_filtered',
    materialized='view'
) }}

select
    index,
    "GEO",
    "WORK_AREA",
    "OBS_VALUE"
from {{ ref('emploi_deplacement_domicile_travail') }}
where "TIME_PERIOD" = 2021
  and "GEO_OBJECT" = 'COM'
  and "WORK_AREA" != '_T'
  and "TRANS" = '_T'

