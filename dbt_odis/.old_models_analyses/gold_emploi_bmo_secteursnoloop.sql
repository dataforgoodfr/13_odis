{{ config(
    tags = ['gold', 'emploi'],
    alias = 'gold_emploi_bmo_secteurs'
) }}

with agg_secteurs as (

    select 
        code_geo as codgeo,
        "NOMBE24" as "Bassin_Emploi",
        "YEAR",
        "Intitulé_FAP22",
        sum(met) as met
        from {{ ref('silver_emploi_bmo') }}
        group by 
            codgeo,
            "NOMBE24",
            "YEAR",
            "Intitulé_FAP22"
)

select
    codgeo,
    "Bassin_Emploi",
    "YEAR",
    max(case when "Intitulé_FAP22" = 'Agriculture, marine, pêche' then met else null end) as "Agriculture, marine, pêche",
    max(case when "Intitulé_FAP22" = 'Artisanat' then met else null end) as "Artisanat",
    max(case when "Intitulé_FAP22" = 'Banque et assurances' then met else null end) as "Banque et assurances",
    max(case when "Intitulé_FAP22" = 'Bâtiments, travaux publics' then met else null end) as "Bâtiments, travaux publics",
    max(case when "Intitulé_FAP22" = 'Commerce' then met else null end) as "Commerce",
    max(case when "Intitulé_FAP22" = 'Communication, information, art et spectacle' then met else null end) as "Communication, information, art et spectacle",
    max(case when "Intitulé_FAP22" = 'Électricité, électronique' then met else null end) as "Électricité, électronique",
    max(case when "Intitulé_FAP22" = 'Enseignement, formation' then met else null end) as "Enseignement, formation",
    max(case when "Intitulé_FAP22" = 'Études et recherche' then met else null end) as "Études et recherche",
    max(case when "Intitulé_FAP22" = 'Gestion et administration des entreprises et des établissements publics' then met else null end) as "Gestion et administration des entreprises et des établissements publics",
    max(case when "Intitulé_FAP22" = 'Hôtellerie, restauration, alimentation' then met else null end) as "Hôtellerie, restauration, alimentation",
    max(case when "Intitulé_FAP22" = 'Industries de process' then met else null end) as "Industries de process",
    max(case when "Intitulé_FAP22" = 'Informatique et télécommunications' then met else null end) as "Informatique et télécommunications",
    max(case when "Intitulé_FAP22" = 'Ingénieurs, cadres et agents des fonctions transverses de l''industrie' then met else null end) as "Ingénieurs, cadres et agents des fonctions transverses de l'industrie",
    max(case when "Intitulé_FAP22" = 'Maintenance' then met else null end) as "Maintenance",
    max(case when "Intitulé_FAP22" = 'Matériaux souples, bois, industries graphiques' then met else null end) as "Matériaux souples, bois, industries graphiques",
    max(case when "Intitulé_FAP22" = 'Mécanique, travail des métaux' then met else null end) as "Mécanique, travail des métaux",
    max(case when "Intitulé_FAP22" = 'Santé, action sociale, culturelle et sportive' then met else null end) as "Santé, action sociale, culturelle et sportive",
    max(case when "Intitulé_FAP22" = 'Services aux particuliers et aux collectivités' then met else null end) as "Services aux particuliers et aux collectivités",
    max(case when "Intitulé_FAP22" = 'Transports, logistique et tourisme' then met else null end) as "Transports, logistique et tourisme"
from agg_secteurs
    group by
        codgeo,
        "Bassin_Emploi",
        "YEAR"
