{{ config(
    tags = ['silver', 'emploi', 'communes_travail'],
    alias='vw_emploi_flux_domicile_travail_stg',
    materialized='view'
) }}
with cleaned_arr as(
select
    index,
    case when "LIBGEO" like 'Paris%'
            then '75056'
        when "LIBGEO" like 'Marseille%'
            then '13055'
        when "LIBGEO" like 'Lyon%'
            then '69123'
        else "CODGEO"
    end as codgeo,
    2022 as "YEAR",
    case when "L_DCLT" like 'Paris%'
            then '75056'
        when "L_DCLT" like 'Marseille%'
            then '13055'
        when "L_DCLT" like 'Lyon%'
            then '69123'
        else "DCLT"
    end as "DCLT",
    case when "L_DCLT" like 'Paris%'
            then 'Paris'
        when "L_DCLT" like 'Marseille%'
            then 'Marseille'
        when "L_DCLT" like 'Lyon%'
            then 'Lyon'
        else "L_DCLT"
    end as "Libellé - lieu de travail",
    "NBFLUX_C22_ACTOCC15P" as "Flux d'actifs de 15 ans ou plus ayant un emploi"
from {{ ref('emploi_flux_domicile_travail') }}
)

select  index,
        codgeo,
        "YEAR",
        "Libellé - lieu de travail",
        "Flux d'actifs de 15 ans ou plus ayant un emploi"
from cleaned_arr
where (codgeo != "DCLT") --remove case where flux is from and to the same city