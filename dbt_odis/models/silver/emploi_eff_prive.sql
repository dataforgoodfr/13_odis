{{ config(
    tags = ['silver', 'emploi'],
    alias='silver_emploi_eff_prive_gds_secteurs'
    )
}}

with 
arr_mapping as (
    select 
        code_geo as arrondissement_code,
        code_aggregation as commune_code
    from {{ ref('geographical_arrondissements') }}
),

communes as (
    select
        c.id,
        c.region,
        c.ancienne_region,
        c.departement,
        c.zone_d_emploi,
        c.epci,
        c.commune,
        c.intitule_commune,
        c.grand_secteur_d_activite,
        c.secteur_na17,
        c.secteur_na38,
        c.secteur_na88,
        c.ape,
        c.code_region,
        c.code_ancienne_region,
        c.code_departement,
        c.code_zone_d_emploi,
        c.code_epci,
        c.code_commune,
        c.code_ape,
        c.annee,
        c.nombre_d_etablissements,
        c.effectifs_salaries,
        coalesce(a.commune_code, c.code_commune) as code_geo,
        case when a.commune_code is not null then 'arrondissement' else 'commune' end as type_geo
    from {{ ref('stg_emploi_eff_prive_pivot') }} c
    left join arr_mapping a
        on c.code_commune = a.arrondissement_code
),

departements as (
    select
        concat(code_ape,'-',code_departement,'-',annee) as id,
        region,
        ancienne_region,
        departement,
        null as zone_d_emploi,
        null as epci,
        null as commune,
        null as intitule_commune,
        grand_secteur_d_activite,
        secteur_na17,
        secteur_na38,
        secteur_na88,
        ape,
        code_region,
        code_ancienne_region,
        code_departement,
        null as code_zone_d_emploi,
        null as code_epci,
        null as code_commune,
        code_ape,
        annee,
        sum(nombre_d_etablissements) as nombre_d_etablissements,
        sum(effectifs_salaries) as effectifs_salaries,
        code_departement as code_geo,
        'departement' as type_geo
    from {{ ref('stg_emploi_eff_prive_pivot') }}
    group by 
        region, 
        ancienne_region, 
        departement, 
        grand_secteur_d_activite, 
        secteur_na17, 
        secteur_na38, 
        secteur_na88, 
        ape, 
        code_region, 
        code_ancienne_region, 
        code_departement, 
        code_ape, 
        annee           
),

regions as (
    select
        concat(code_ape,'-',code_region,'-',annee) as id,
        region,
        null as ancienne_region,
        null as departement,
        null as zone_d_emploi,
        null as epci,
        null as commune,
        null as intitule_commune,
        grand_secteur_d_activite,
        secteur_na17,
        secteur_na38,
        secteur_na88,
        ape,
        code_region,
        null as code_ancienne_region,
        null as code_departement,
        null as code_zone_d_emploi,
        null as code_epci,
        null as code_commune,
        code_ape,
        annee,
        sum(nombre_d_etablissements) as nombre_d_etablissements,
        sum(effectifs_salaries) as effectifs_salaries,
        code_region as code_geo,
        'region' as type_geo
    from {{ ref('stg_emploi_eff_prive_pivot') }}
    group by 
        region, 
        grand_secteur_d_activite, 
        secteur_na17, 
        secteur_na38, 
        secteur_na88, 
        ape, 
        code_region, 
        code_ape, 
        annee           
)

select * from communes
union all
select * from departements
union all
select * from regions




