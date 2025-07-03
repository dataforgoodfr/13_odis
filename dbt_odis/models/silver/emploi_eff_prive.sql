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
        coalesce(a.commune_code, c.codgeo) as code_geo,
        case when a.commune_code is not null then 'arrondissement' else 'commune' end as type_geo
    from {{ ref('stg_emploi_eff_prive_pivot') }} c
    left join arr_mapping a
        on b.codgeo = a.arrondissement_code
)

select
* from communes
