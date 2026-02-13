{{ config(
    tags = ['gold', 'homepage'],
    alias = 'gold_presentation_page_epci'
) }}

SELECT
    codgeo,
    raison_sociale,
    nb_membres,
    total_ptot_communes as total_pop_tot,
    total_pmun_communes as total_pop_mun,
    ptot_commune as ptot_2023,
    pmun_commune as pmun_2023
FROM {{ ref('silver_presentation_page_epci') }}