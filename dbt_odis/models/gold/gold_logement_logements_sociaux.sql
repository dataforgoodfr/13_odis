{{ config(
    alias='gold_logement_logements_sociaux',
    tags=['gold', 'logement_social']
) }}

select
    codgeo,
    year,
    coalesce(nb_ls, 0) as "Parc complet Ensemble du parc social",
    coalesce(nb_loues, 0) as "Parc complet proposés à la location loués",
    coalesce(nb_vacants, 0) as "Parc complet proposés à la location vacants",
    coalesce(nb_vides, 0) as "Parc complet vide",
    coalesce(nb_asso, 0) as "Parc complet pris en charge par une association",
    coalesce(nb_occup_finan, 0) as "Parc complet occupés avec ou sans contrepartie financière",
    coalesce(nb_occup_temp, 0) as "Parc complet occupé pour de l'hébergement temporaire",
    coalesce(nb_ls_en_qpv, 0) as "Parc complet Nombre de logements en QPV",
    coalesce(tx_vac, 0) as "Taux de vacance % totale Au 01/01",
    coalesce(tx_vac3, 0) as "Taux de vacance % > 3 mois Au 01/01",
    coalesce(tx_mob, 0) as "Taux de mobilité % Au 01/01",
    coalesce(densite, 0) as "Parc complet Densité pour 100 résidences principales"
  from {{ ref('silver_logement_logements_sociaux') }}
