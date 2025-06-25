{{ config(
    alias='silver_logement_logements_sociaux',
    tags=['silver', 'logement_social']
) }}

with

communes as (
  select
    type_geo,
    codgeo,
    libcom as lib_geo,
    year,
    nb_ls,
    tx_vac,
    tx_vac3,
    tx_mob,
    nb_loues,
    nb_vacants,
    nb_vides,
    nb_asso,
    nb_occup_finan,
    nb_occup_temp,
    nb_ls_en_qpv,
    densite
  from {{ ref('stg_logement_logements_sociaux_communes') }}
),

departement as (
  select
    type_geo,
    codgeo,
    libdep as lib_geo,
    year,
    nb_ls,
    tx_vac,
    tx_vac3,
    tx_mob,
    nb_loues,
    nb_vacants,
    nb_vides,
    nb_asso,
    nb_occup_finan,
    nb_occup_temp,
    nb_ls_en_qpv,
    densite
  from {{ ref('stg_logement_logements_sociaux_departement') }}
),

region as (
  select
    type_geo,
    codgeo,
    libreg as lib_geo,
    year,
    nb_ls,
    tx_vac,
    tx_vac3,
    tx_mob,
    nb_loues,
    nb_vacants,
    nb_vides,
    nb_asso,
    nb_occup_finan,
    nb_occup_temp,
    nb_ls_en_qpv,
    densite
  from {{ ref('stg_logement_logements_sociaux_region') }}
),

epci as (
  select
    type_geo,
    codgeo,
    libepci as lib_geo,
    year,
    nb_ls,
    tx_vac,
    tx_vac3,
    tx_mob,
    nb_loues,
    nb_vacants,
    nb_vides,
    nb_asso,
    nb_occup_finan,
    nb_occup_temp,
    nb_ls_en_qpv,
    densite
  from {{ ref('stg_logement_logements_sociaux_epci') }}
)

select * from communes
    union all
select * from departement
    union all
select * from region
    union all
select * from epci



