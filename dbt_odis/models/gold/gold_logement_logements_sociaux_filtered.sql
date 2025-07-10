{{ config(
    alias='gold_logement_logements_sociaux_filtered',
    tags=['gold', 'logement_social']
) }}

with annee24 as (
    select
        codgeo,
        year,
        nb_ls,
        nb_loues,
        nb_vacants,
        nb_vides,
        nb_asso,
        nb_occup_finan,
        nb_occup_temp,
        nb_ls_en_qpv,
        tx_vac,
        tx_vac3,
        tx_mob
    from {{ ref('silver_logement_logements_sociaux') }}
    where year = 2024
),

densite21 as (
    select
        codgeo,
        year,
        densite
    from {{ ref('silver_logement_logements_sociaux') }}
    where year = 2021
)

select
    a.codgeo,
    a.year,
    coalesce(a.nb_ls, 0) as "Parc complet Ensemble du parc social",
    coalesce(a.nb_loues, 0) as "Parc complet proposés à la location loués",
    coalesce(a.nb_vacants, 0) as "Parc complet proposés à la location vacants",
    coalesce(a.nb_vides, 0) as "Parc complet vide",
    coalesce(a.nb_asso, 0) as "Parc complet pris en charge par une association",
    coalesce(a.nb_occup_finan, 0) as "Parc complet occupés avec ou sans contrepartie financière",
    coalesce(a.nb_occup_temp, 0) as "Parc complet occupé pour de l'hébergement temporaire",
    coalesce(a.nb_ls_en_qpv, 0) as "Parc complet Nombre de logements en QPV",
    coalesce(a.tx_vac, 0) as "Taux de vacance % totale Au 01/01",
    coalesce(a.tx_vac3, 0) as "Taux de vacance % > 3 mois Au 01/01",
    coalesce(a.tx_mob, 0) as "Taux de mobilité % Au 01/01",
    coalesce(d.densite, 0) as "Parc complet Densité pour 100 résidences principales"
  from annee24 a
  left join densite21 d
    on a.codgeo = d.codgeo
