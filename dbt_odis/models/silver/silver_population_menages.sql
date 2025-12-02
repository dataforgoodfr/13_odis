{{ config(
    tags = ['silver', 'population'],
    alias='silver_population_nb_menages'
    )
}}

with population_nb_menages as (
    select 
        codgeo,
        2021 as YEAR,
        SUM(NB) as Nb_Menages, 
        SUM(NPERC * NB) / nullif(SUM(NB),0) as Nb_Occ_Moyen 
    from {{ ref('population_nb_menages') }}
)

select * from population_nb_menages