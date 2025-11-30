{{ config(
    tags = ['silver', 'homepage'],
    alias = 'silver_presentation_page_epci'
) }}

WITH population_communes AS (
    SELECT
        RIGHT(population_totale.geo, 5)::VARCHAR AS code_insee,
        SUM(
            CASE
                WHEN population_totale.popref_measure = 'PTOT' THEN value::int ELSE 0
            END
         ) AS ptot_commune,
        SUM(
            CASE
                WHEN population_totale.popref_measure = 'PMUN' THEN value::int ELSE 0
            END
         ) AS pmun_commune
    FROM {{ ref('population_totale') }} population_totale
    GROUP BY RIGHT(population_totale.geo, 5)::VARCHAR
)
, agg_communes AS (
    SELECT
        communes.epci_code,
        COUNT(communes.*) AS nb_membres,
        SUM(communes.population::int) AS population_totale_communes,
        SUM(population_communes.ptot_commune) AS total_ptot_communes,
        SUM(population_communes.pmun_commune) AS total_pmun_communes
    FROM {{ ref('geographical_references_communes') }} communes
    INNER JOIN population_communes
	    ON population_communes.code_insee = communes.code
    GROUP BY epci_code
)
SELECT
    communes.code AS codgeo,
    communes.nom,
    communes.epci_nom AS raison_sociale,
    population_communes.ptot_commune,
    population_communes.pmun_commune,
    agg_communes.nb_membres,
    agg_communes.total_ptot_communes,
    agg_communes.total_pmun_communes,
    communes.population AS pmun_source_geo_communes,
    agg_communes.population_totale_communes AS agg_pmun_source_geo_communes,
    epci.population AS agg_pmun_source_epci
FROM {{ ref('geographical_references_communes') }} communes
INNER JOIN population_communes
    ON population_communes.code_insee = communes.code
INNER JOIN agg_communes
    ON agg_communes.epci_code = communes.epci_code
LEFT JOIN {{ ref('presentation_page_epci') }} epci
    ON epci.code = communes.epci_code
