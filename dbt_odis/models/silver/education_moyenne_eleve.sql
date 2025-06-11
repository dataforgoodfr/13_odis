{{ config(
    tags = ['silver', 'education'],
    alias = 'silver_education_moyenne_eleve'
    )
}}

with geo_commune as (
select
    "Code_commune_INSEE" as code_geo,
    "Nom_de_la_commune" as nom,
    "Code_postal" as code_postal,
    "Intitule" as commune
from {{ ref('corresp_codes_communes') }}
group by 
    code_geo,
    nom,
    code_postal,
    commune
),
    
education_communes as (
    select 
        id,
        code_postal,
        commune,
        departement,
        academie,
        region_academique,
        rentree_scolaire,
        nombre_total_eleves,
        nombre_total_classes,
        nombre_eleves_ulis,
        nombre_eleves_cp_hors_ulis,
        nombre_eleves_ce1_hors_ulis,
        nombre_eleves_ce2_hors_ulis,
        nombre_eleves_cm1_hors_ulis,
        nombre_eleves_cm2_hors_ulis,
        nombre_eleves_elementaire_hors_ulis,
        nombre_eleves_preelementaire_hors_ulis,
        created_at,
        regexp_replace(commune,'[-'']', ' ', 'g') as nom_commune
    from {{ ref('education_moyenne_eleve_commune') }}
),

join_commune as(
    select 
        gc.code_geo,
        c.id,
        c.code_postal,
        c.commune as nom_geo,
        'commune' as type_geo,
        c.departement,
        c.academie,
        c.region_academique,
        c.rentree_scolaire,
        c.nombre_total_eleves,
        c.nombre_total_classes,
        c.nombre_eleves_ulis,
        c.nombre_eleves_cp_hors_ulis,
        c.nombre_eleves_ce1_hors_ulis,
        c.nombre_eleves_ce2_hors_ulis,
        c.nombre_eleves_cm1_hors_ulis,
        c.nombre_eleves_cm2_hors_ulis,
        c.nombre_eleves_elementaire_hors_ulis,
        c.nombre_eleves_preelementaire_hors_ulis,
        c.created_at
    from education_communes c
        left join geo_commune gc
        on c.code_postal = gc.code_postal
        and '%' || c.nom_commune || '%' like '%' || gc.commune || '%'
),

geo_departement as (
    select 
        code,
        intitule,
        regexp_replace(upper(translate(intitule, 
                'àâäáãåçéèêëíìîïñóòôöõúùûüýÿÀÂÄÁÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕÚÙÛÜÝŸ',
                'aaaaaaceeeeiiiinooooouuuuyyAAAAAACEEEEIIIINOOOOOUUUUYY')),
                '[-'']', ' ', 'g') as departement
    from {{ ref('geographical_references_departements') }}
),

education_dep as (
    select 
        id,
        case 
            when departement = 'ALPES-DE-HTE-PROVENCE' then 'ALPES-DE-HAUTE-PROVENCE'
            else departement
        end as departement,
        academie,
        region_academique,
        rentree_scolaire,
        nombre_total_eleves,
        nombre_total_classes,
        nombre_eleves_ulis,
        nombre_eleves_cp_hors_ulis,
        nombre_eleves_ce1_hors_ulis,
        nombre_eleves_ce2_hors_ulis,
        nombre_eleves_cm1_hors_ulis,
        nombre_eleves_cm2_hors_ulis,
        nombre_eleves_elementaire_hors_ulis,
        nombre_eleves_preelementaire_hors_ulis,
        created_at,
        regexp_replace(
            case 
                when departement = 'ALPES-DE-HTE-PROVENCE' then 'ALPES-DE-HAUTE-PROVENCE'
                else departement
            end,
            '[-'']', ' ', 'g') as nom_departement
    from {{ ref('education_moyenne_eleve_departement') }}
),

join_departement as (
    select 
        gd.code as code_geo,
        d.id,
        gd.code as code_postal,
        gd.intitule as nom_geo,
        'departement' as type_geo,
        d.departement,
        d.academie,
        d.region_academique,
        d.rentree_scolaire,
        d.nombre_total_eleves,
        d.nombre_total_classes,
        d.nombre_eleves_ulis,
        d.nombre_eleves_cp_hors_ulis,
        d.nombre_eleves_ce1_hors_ulis,
        d.nombre_eleves_ce2_hors_ulis,
        d.nombre_eleves_cm1_hors_ulis,
        d.nombre_eleves_cm2_hors_ulis,
        d.nombre_eleves_elementaire_hors_ulis,
        d.nombre_eleves_preelementaire_hors_ulis,
        d.created_at
    from education_dep as d
    left join geo_departement as gd
        on d.nom_departement = gd.departement
),

geo_regions as (
    select 
        code,
        intitule,
        regexp_replace(upper(translate(intitule, 
                'àâäáãåçéèêëíìîïñóòôöõúùûüýÿÀÂÄÁÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕÚÙÛÜÝŸ',
                'aaaaaaceeeeiiiinooooouuuuyyAAAAAACEEEEIIIINOOOOOUUUUYY')),
                '[-'']', ' ', 'g') as region
    from {{ ref('geographical_references_regions') }}
),

education_regions as (
    select 
        id,
        region_academique,
        rentree_scolaire,
        nombre_total_eleves,
        nombre_total_classes,
        nombre_eleves_ulis,
        nombre_eleves_cp_hors_ulis,
        nombre_eleves_ce1_hors_ulis,
        nombre_eleves_ce2_hors_ulis,
        nombre_eleves_cm1_hors_ulis,
        nombre_eleves_cm2_hors_ulis,
        nombre_eleves_elementaire_hors_ulis,
        nombre_eleves_preelementaire_hors_ulis,
        created_at,
        regexp_replace(region_academique,'[-'']', ' ', 'g') as nom_region
    from {{ ref('education_moyenne_eleve_region') }}
),

join_region as (
    select 
        gr.code as code_geo,
        r.id,
        gr.code as code_postal,
        gr.intitule as nom_geo,
        'region' as type_geo,
        NULL as departement,
        NULL as academie,
        r.region_academique,
        r.rentree_scolaire,
        r.nombre_total_eleves,
        r.nombre_total_classes,
        r.nombre_eleves_ulis,
        r.nombre_eleves_cp_hors_ulis,
        r.nombre_eleves_ce1_hors_ulis,
        r.nombre_eleves_ce2_hors_ulis,
        r.nombre_eleves_cm1_hors_ulis,
        r.nombre_eleves_cm2_hors_ulis,
        r.nombre_eleves_elementaire_hors_ulis,
        r.nombre_eleves_preelementaire_hors_ulis,
        r.created_at
    from education_regions as r
    left join geo_regions as gr
        on r.nom_region = gr.region
)

select * from join_commune
    union all
select * from join_departement
    union all
select * from join_region

