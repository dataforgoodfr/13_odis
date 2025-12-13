{{ config(
    tags = ['silver', 'population'],
    alias='silver_population_menages'
    )
}}

with population_nb_menages as (
    select 
        "CODGEO" as codgeo,
        2021 as year,
        SUM("NB") as nb_menages, 
        SUM("NPERC" * "NB") / nullif(SUM("NB"),0) as nb_occ_Moyen 
    from {{ ref('population_menages_2021') }}
    group by "CODGEO"
)

select * from population_nb_menages