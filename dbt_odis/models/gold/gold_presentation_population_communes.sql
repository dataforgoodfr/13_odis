{{ config(
    tags = ['gold', 'population', 'presentation'],
    alias='gold_presentation_population_communes',
) }}


select
    codgeo,
    population as population_totale,
    year
from {{ ref("stg_population_population_superficie") }}
where codgeo_type = 'COM'