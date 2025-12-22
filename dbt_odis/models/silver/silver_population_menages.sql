{{ config(
    tags = ['silver', 'population'],
    alias='silver_population_menages'
    )
}}

with population_nb_menages as (
    select
        coalesce(passage."Code Courant Officiel", pop."CODGEO") as codgeo,
        pop.year,
        SUM(pop."NB") as nb_menages,
        SUM(pop."NPERC" * pop."NB") / nullif(SUM(pop."NB"),0) as nb_occ_Moyen,
        'COM' as niveau_geo
    from {{ ref('population_menages_2021') }} pop
    left join {{ ref('geocodes_passage_annuel') }} passage
        on pop."CODGEO" = passage."Ancien Code Officiel"
        and passage."Niveau" = 'COM'
    where pop."NIVGEO" = 'COM'
    group by coalesce(passage."Code Courant Officiel", pop."CODGEO"), pop.year
)

select * from population_nb_menages