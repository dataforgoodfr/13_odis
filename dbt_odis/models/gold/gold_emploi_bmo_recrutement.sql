{{ config(
    tags = ['gold', 'emploi'],
    alias = 'gold_emploi_bmo_recrutement',
) }}

with agg_codgeo as (

    select 
        code_geo as codgeo,
        "NOMBE24" as "Bassin_Emploi",
        "YEAR",
        sum(met) as "Projets de recrutement",
        sum(xmet) as "Difficultés à recruter",
        sum(smet) as "Emplois saisonniers"
        from {{ ref('silver_emploi_bmo') }}
        group by 
            codgeo,
            "NOMBE24",
            "YEAR"
)
 
select
    codgeo,
    "Bassin_Emploi",
    "YEAR",
    "Projets de recrutement",
    ROUND(("Difficultés à recruter" * 100.0 / NULLIF("Projets de recrutement", 0)), 1) AS "Difficultés à recruter",
    ROUND(("Emplois saisonniers" * 100.0 / NULLIF("Projets de recrutement", 0)), 1) AS "Emplois saisonniers"
FROM agg_codgeo