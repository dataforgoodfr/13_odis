{{ config(
    tags=['silver', 'population'],
    alias='vw_population_population_superficie_stg',
    materialized='view'
) }}

with pivot_pop_sup as
(
    select
        split_part("geo", '-', 1) as codgeo_year,
        split_part("geo", '-', 2) as codgeo_type, -- REG, COM, DEP, etc.
        split_part("geo", '-', 3) as codgeo,
        time_period as "year",
        -- pivot population et superficie
        cast(max(case when rp_measure = 'POP' then split_part(measure_value, '.', 1) end) as int) as population,
        cast(max(case when rp_measure = 'SUP' then split_part(measure_value, '.', 1) end) as int) as superficie
    from {{ ref('population_population_superficie') }}
    -- filtres presents dans datasource.yml, mais explicites ici pour clarte
    where split_part("geo", '-', 2) in ('COM', 'REG', 'DEP')
    and ocs = '_T'
    group by
        split_part(geo, '-', 1),
        split_part(geo, '-', 2),
        split_part(geo, '-', 3),
        time_period
)

select * from pivot_pop_sup