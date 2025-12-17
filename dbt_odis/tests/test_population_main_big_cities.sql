
with insee_data as (
    select * from {{ ref('insee_top_50_communes_par_habitants') }}
)

select
    id."GEO" as codgeo,
    id."OBS_VALUE" as insee_population,
    model.population_totale as model_population,
    id."OBS_VALUE" / model.population_totale as ratio
from insee_data id
left join {{ ref('gold_presentation_population_communes') }} model
    on id."GEO" = model.codgeo
where model.population_totale is not null
    and model.population_totale > 0
    and (
        id."OBS_VALUE" / model.population_totale < 0.9
        or id."OBS_VALUE" / model.population_totale > 1.1
    )