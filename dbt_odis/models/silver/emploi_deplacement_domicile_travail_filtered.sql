{{ config(
    tags = ['silver', 'emploi', 'communes_travail'],
    alias='vw_emploi_deplacement_domicile_travail_filtered',
    materialized='view'
) }}

select
    index,
    "GEO",
    "WORK_AREA",
    "OBS_VALUE",
    "TIME_PERIOD"
from {{ ref('emploi_deplacement_domicile_travail') }}
where "GEO_OBJECT" = 'COM'
  and "WORK_AREA" != '_T'
  and "TRANS" = '_T'

