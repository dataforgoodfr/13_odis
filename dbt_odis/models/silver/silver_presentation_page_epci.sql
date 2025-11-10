{{ config(
    tags = ['silver', 'homepage'],
    alias = 'silver_presentation_page_epci'
) }}

WITH nb_membres AS (
    SELECT
        epci_code,
        COUNT(*) AS nb_membres,
        SUM(population::int) AS population_totale
    FROM {{ ref('geographical_references_communes') }} communes
    GROUP BY epci_code
)
SELECT
    communes.code AS codgeo,
    communes.nom,
    communes.epci_nom AS raison_sociale,
    nb_membres.nb_membres,
    -- nb_membres.population_totale AS total_pop_tot,
    epci.population AS total_pop_mun,
    communes.population AS pmun_derniere_annee
FROM {{ ref('geographical_references_communes') }} communes
INNER JOIN nb_membres
    ON nb_membres.epci_code = communes.epci_code
LEFT JOIN {{ ref('presentation_page_epci') }} epci
    ON epci.code = communes.epci_code
WHERE TRUE
;
