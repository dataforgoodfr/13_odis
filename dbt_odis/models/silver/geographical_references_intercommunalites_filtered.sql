{{ config(
    tags = ['silver', 'geographical'],
    alias='geographical_references_intercommunalites_filtered'
    )
}}

select 
    n_siren,
    nom_du_groupement,
    population_totale,
    nombre_de_membres,
    siren_membre,
    nom_membre,
    categorie_des_membres_du_groupement,
    population_totale_du_membre_du_groupement 
from {{ ref("geographical_references_intercommunalites") }}