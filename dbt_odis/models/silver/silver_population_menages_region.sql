{{ config(
    tags = ['silver', 'population'],
    alias='silver_population_menages_region'
    )
}}

with population_nb_menages_reg as (
    select
        'reg' || geo."CODREG" as codgeo,
        2021 as year,
        SUM(pop."NB") as nb_menages,
        SUM(pop."NPERC" * pop."NB") / nullif(SUM(pop."NB"),0) as nb_occ_Moyen,
        'REG' as niveau_geo
    from {{ ref('population_menages_2021') }} pop
    inner join {{ ref('com_dep_reg') }} geo on pop."CODGEO" = geo."CODGEO"
    where pop."NIVGEO" = 'COM'
    group by geo."CODREG"
)

select * from population_nb_menages_reg
