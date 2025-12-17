{{ config(
    tags = ['silver', 'population'],
    alias='silver_population_menages'
    )
}}

with population_nb_menages as (
    select
        "CODGEO" as codgeo,
        year,
        SUM("NB") as nb_menages,
        SUM("NPERC" * "NB") / nullif(SUM("NB"),0) as nb_occ_Moyen,
        'COM' as niveau_geo
    from {{ ref('population_menages_2021') }} where "NIVGEO" = 'COM'
    group by "CODGEO", year
)

select * from population_nb_menages