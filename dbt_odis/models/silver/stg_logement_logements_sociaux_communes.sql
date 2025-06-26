{{ config(
    alias='stg_logement_logements_sociaux_communes',
    tags=['silver', 'logement_social']
) }}

with

-- Historical indicators pivoted long with multiple years
nb_ls as (
  {{ build_historical_indicator_cte(
      ref('logement_logements_sociaux_communes'),
      ['codgeo', 'libcom', 'dep', 'reg'],
      'nb_ls',
      'nb_ls',
      exclusion_prefixes=[]
  ) }}
),

tx_vac as (
  {{ build_historical_indicator_cte(
      ref('logement_logements_sociaux_communes'),
      ['codgeo', 'libcom', 'dep', 'reg'],
      'tx_vac',
      'tx_vac',
      exclusion_prefixes=['tx_vac_3']
  ) }}
),

tx_vac3 as (
  {{ build_historical_indicator_cte(
      ref('logement_logements_sociaux_communes'),
      ['codgeo', 'libcom', 'dep', 'reg'],
      'tx_vac3',
      'tx_vac_3',
      exclusion_prefixes=[]
  ) }}
),

tx_mob as (
  {{ build_historical_indicator_cte(
      ref('logement_logements_sociaux_communes'),
      ['codgeo', 'libcom', 'dep', 'reg'],
      'tx_mob',
      'tx_mob',
      exclusion_prefixes=[]
  ) }}
),

-- Static indicators only present for next_year (e.g. nb_loues, nb_vacants, etc.)
static_indicators as (
  {{ build_static_indicator_cte(
      ref('logement_logements_sociaux_communes'),
      ['codgeo', 'libcom', 'dep', 'reg'],
      ['nb_loues', 'nb_vacants', 'nb_vides', 'nb_asso', 'nb_occup_finan', 'nb_occup_temp', 'nb_ls_en_qpv']
  ) }}
),

-- Densite block with fixed year (2021, from last census as of 01/01/2025)
densite_block as (
  select
    codgeo,
    libcom,
    dep,
    reg,
    2021 as year,
    densite
  from {{ ref('logement_logements_sociaux_communes') }}
),

-- Join all historical indicators + static indicators on geo + year
joined_indicators as (
  select
    l.codgeo,
    l.libcom,
    l.dep,
    l.reg,
    l.year,
    l.indicateur as nb_ls,
    tv.indicateur as tx_vac,
    tv3.indicateur as tx_vac3,
    tm.indicateur as tx_mob,
    s.nb_loues,
    s.nb_vacants,
    s.nb_vides,
    s.nb_asso,
    s.nb_occup_finan,
    s.nb_occup_temp,
    s.nb_ls_en_qpv
  from nb_ls l
  left join tx_vac tv
    on l.codgeo = tv.codgeo and l.year = tv.year
  left join tx_vac3 tv3
    on l.codgeo = tv3.codgeo and l.year = tv3.year
  left join tx_mob tm
    on l.codgeo = tm.codgeo and l.year = tm.year
  left join static_indicators s
    on l.codgeo = s.codgeo and l.year = s.year
),

-- Add densite and type_geo
final as (
  select
    j.codgeo,
    j.libcom,
    j.dep,
    j.reg,
    j.year,
    'commune' as type_geo,
    j.nb_ls,
    j.tx_vac,
    j.tx_vac3,
    j.tx_mob,
    j.nb_loues,
    j.nb_vacants,
    j.nb_vides,
    j.nb_asso,
    j.nb_occup_finan,
    j.nb_occup_temp,
    j.nb_ls_en_qpv,
    d.densite
  from joined_indicators j
  left join densite_block d
    on j.codgeo = d.codgeo and j.year = d.year
)

select * from final
