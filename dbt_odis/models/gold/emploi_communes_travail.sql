{{ config(
    tags = ['gold', 'emploi', 'communes_travail'],
    alias = 'gold_emploi_communes_travail'
    )
}}


select 

    codgeo,
    "YEAR",
    "Libellé - lieu de travail",
    sum("Flux d'actifs de 15 ans ou plus ayant un emploi") as "Flux d'actifs de 15 ans ou plus ayant un emploi"

 from {{ref("stg_emploi_flux_domicile_travail")}}
 group by codgeo, "YEAR", "Libellé - lieu de travail"

