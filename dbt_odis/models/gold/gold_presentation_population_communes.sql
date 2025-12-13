{{ config(
    tags = ['gold', 'population', 'presentation'],
    alias='vw_presentation_population_communes_gold',
) }}


select codgeo, population_totale, year from {{ ref("stg_presentation_population_communes") }}