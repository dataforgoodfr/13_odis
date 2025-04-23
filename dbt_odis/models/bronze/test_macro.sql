select
  data,
  data->'dimensions' as dimensions,
  data->'dimensions'->>'GEO' as geo,
  data->'measures'->'OBS_VALUE_NIVEAU'->>'value' as obs_value
from {{ source('bronze', 'logement_logements_appartement_et_residences_principales') }}
