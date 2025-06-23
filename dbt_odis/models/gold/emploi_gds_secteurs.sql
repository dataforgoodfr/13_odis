{{ config(
    tags = ['gold', 'emploi'],
    alias='gold_emploi_eff_secteur_prive_gds_secteurs'
) }}

with base as (
    select
        code_commune as codgeo,
        annee as year,
        grand_secteur_d_activite,
        nombre_d_etablissements,
        effectifs_salaries
    from {{ ref('emploi_eff_prive') }}
),

etablissements as (
    select
        codgeo,
        year,
        'nombre_d_etablissements' as type,
        sum(case when grand_secteur_d_activite = 'GS1 Industrie' then nombre_d_etablissements else 0 end) as "GS1 Industrie",
        sum(case when grand_secteur_d_activite = 'GS2 Construction' then nombre_d_etablissements else 0 end) as "GS2 Construction",
        sum(case when grand_secteur_d_activite = 'GS3 Commerce' then nombre_d_etablissements else 0 end) as "GS3 Commerce",
        sum(case when grand_secteur_d_activite = 'GS4 Hôtellerie-restauration' then nombre_d_etablissements else 0 end) as "GS4 Hôtellerie-restauration",
        sum(case when grand_secteur_d_activite = 'GS5 Autres services marchands hors intérim' then nombre_d_etablissements else 0 end) as "GS5 Autres services marchands hors intérim",
        sum(case when grand_secteur_d_activite = 'GS6 Intérim' then nombre_d_etablissements else 0 end) as "GS6 Intérim",
        sum(case when grand_secteur_d_activite = 'GS7 Services non marchands' then nombre_d_etablissements else 0 end) as "GS7 Services non marchands"
    from base
    group by codgeo, year
),

effectifs as (
    select
        codgeo,
        year,
        'effectifs_salaries' as type,
        sum(case when grand_secteur_d_activite = 'GS1 Industrie' then effectifs_salaries else 0 end) as "GS1 Industrie",
        sum(case when grand_secteur_d_activite = 'GS2 Construction' then effectifs_salaries else 0 end) as "GS2 Construction",
        sum(case when grand_secteur_d_activite = 'GS3 Commerce' then effectifs_salaries else 0 end) as "GS3 Commerce",
        sum(case when grand_secteur_d_activite = 'GS4 Hôtellerie-restauration' then effectifs_salaries else 0 end) as "GS4 Hôtellerie-restauration",
        sum(case when grand_secteur_d_activite = 'GS5 Autres services marchands hors intérim' then effectifs_salaries else 0 end) as "GS5 Autres services marchands hors intérim",
        sum(case when grand_secteur_d_activite = 'GS6 Intérim' then effectifs_salaries else 0 end) as "GS6 Intérim",
        sum(case when grand_secteur_d_activite = 'GS7 Services non marchands' then effectifs_salaries else 0 end) as "GS7 Services non marchands"
    from base
    group by codgeo, year
),

unioned as (
    select * from etablissements
    union all
    select * from effectifs
)

select
    codgeo,
    year,
    case
        when type = 'nombre_d_etablissements' then 'Nombre d''établissements'
        when type = 'effectifs_salaries' then 'Effectifs salariés'
    end as type,
    "GS1 Industrie",
    "GS2 Construction",
    "GS3 Commerce",
    "GS4 Hôtellerie-restauration",
    "GS5 Autres services marchands hors intérim",
    "GS6 Intérim",
    "GS7 Services non marchands"
from unioned
