-- Test that TP40, TP50, TP60 are all valid floats (null or numeric)
  select
      "TP40",
      "TP50",
      "TP60"
  from {{ ref('stg_population_taux_pauvrete') }}
  where
      (("TP40" is not null and pg_typeof("TP40")::text != 'double precision')
      or ("TP50" is not null and pg_typeof("TP50")::text != 'double precision')
      or ("TP60" is not null and pg_typeof("TP60")::text != 'double precision'))