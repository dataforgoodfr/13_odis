{{ config(
    tags = ['gold', 'geographical'],
    alias='geographical_references_intercommunalites_renamed'
    )
}}

select
    siren_membre as codgeo,
    nom_du_groupement as raison_social,
    nombre_de_membres as nb_membres,
    population_totale as total_pop_tot,
    population_totale_du_membre_du_groupement as total_pop_mun
from {{ ref("geographical_references_intercommunalites_filtered") }}
