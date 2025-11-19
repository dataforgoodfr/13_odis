{{ config(
    tags = ['silver', 'population', 'presentation'],
    alias='vw_presentation_population_communes_stg',
    materialized='view'
) }}

with cleaned as (
    select 
        split_part("GEO", '-', 3) as codgeo,
        "value"::float as population_totale,
        "TIME_PERIOD"::integer as year,
        "GEO" as geo_original
    from {{ ref("presentation_population_communes") }}
    where "GEO" like '%COM-%'
        and "AGE" = '_T'
        and "SEX" = '_T'
        and "RP_MEASURE" = 'POP'
)

select * from cleaned