{{ config(
    tags = ['gold', 'population', 'presentation'],
    alias='vw_presentation_population_communes_gold',
) }}


select
    codgeo,
    population as population_totale,
    year
from {{ ref("stg_population_population_superficie") }}
where codgeo_type = 'COM'