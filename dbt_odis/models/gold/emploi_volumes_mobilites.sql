{{ config(
    tags = ['gold', 'emploi'],
    alias = 'gold_emploi_volumes_mobilites'
    )
}}

with source_data as (

    select
        "GEO" as codgeo,
        "WORK_AREA",
        "OBS_VALUE"
    from {{ ref('emploi_deplacement_domicile_travail_filtered') }}
    where "WORK_AREA" in ('10', '20_30')

),

pivoted as (

    select
        codgeo,
        2021 as YEAR,
        max(case when "WORK_AREA" = '10' then "OBS_VALUE" end) as "'Nb actifs travaillant dans commune'",
        max(case when "WORK_AREA" = '20_30' then "OBS_VALUE" end) as "'Nb actifs travaillant dans autre commune'"
    from source_data
    group by codgeo

)

select * from pivoted

