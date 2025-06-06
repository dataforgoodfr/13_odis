{{ config(
    alias = 'silver_education_moyenne_eleve'
    )
}}



Select * FROM {{ ref('geographical_references_communes') }} 
Select * FROM {{ ref('education_moyenne_eleve_commune') }} AS B ORDER BY commune ASC

Select * FROM {{ ref('geographical_references_departements') }} 
Select * FROM {{ ref('education_moyenne_eleve_departement') }} AS C

Select * FROM {{ ref('geographical_references_regions') }} 
Select * FROM {{ ref('education_moyenne_eleve_region') }} AS D



WITH geo_communes AS (
    SELECT 
        code,
        UPPER(TRANSLATE(nom, 'àâäáãåçéèêëíìîïñóòôöõúùûüýÿ', 'aaaaaaceeeeiiiinooooouuuuyy')) AS commune
    FROM {{ ref('geographical_references_communes') }}
),

education_communes AS (
    SELECT 
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
        created_at
    FROM {{ ref('education_moyenne_eleve_commune') }}
)



SELECT 
    g.code AS code_insee,
    e.id,
    e.code_postal,
    e.commune,
    e.departement,
    e.academie,
    e.region_academique,
    e.rentree_scolaire,
    e.nombre_total_eleves,
    e.nombre_total_classes,
    e.nombre_eleves_ulis,
    e.nombre_eleves_cp_hors_ulis,
    e.nombre_eleves_ce1_hors_ulis,
    e.nombre_eleves_ce2_hors_ulis,
    e.nombre_eleves_cm1_hors_ulis,
    e.nombre_eleves_cm2_hors_ulis,
    e.nombre_eleves_elementaire_hors_ulis,
    e.nombre_eleves_preelementaire_hors_ulis,
    e.created_at    
FROM education_communes AS e
LEFT JOIN geo_communes AS g
    ON e.commune = UPPER(TRANSLATE(g.commune, 'àâäáãåçéèêëíìîïñóòôöõúùûüýÿ', 'aaaaaaceeeeiiiinooooouuuuyy'))


WITH geo_departement AS (
    SELECT 
        code,
        UPPER(TRANSLATE(nom, 'àâäáãåçéèêëíìîïñóòôöõúùûüýÿ', 'aaaaaaceeeeiiiinooooouuuuyy')) AS commune
    FROM {{ ref('geographical_references_departements') }}
),

education_dep AS (
    SELECT 
        id,
        NULL AS code_postal,
        NULL AS commune,
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
        created_at
    FROM {{ ref('education_moyenne_eleve_departement') }}
)

SELECT 
    h.code AS code_insee,
    f.id,
    f.code_postal,
    f.commune,
    f.departement,
    f.academie,
    f.region_academique,
    f.rentree_scolaire,
    f.nombre_total_eleves,
    f.nombre_total_classes,
    f.nombre_eleves_ulis,
    f.nombre_eleves_cp_hors_ulis,
    f.nombre_eleves_ce1_hors_ulis,
    f.nombre_eleves_ce2_hors_ulis,
    f.nombre_eleves_cm1_hors_ulis,
    f.nombre_eleves_cm2_hors_ulis,
    f.nombre_eleves_elementaire_hors_ulis,
    f.nombre_eleves_preelementaire_hors_ulis,
    f.created_at    
FROM education_dep AS f
LEFT JOIN geo_departement AS h
    ON f.commune = UPPER(TRANSLATE(h.commune, 'àâäáãåçéèêëíìîïñóòôöõúùûüýÿ', 'aaaaaaceeeeiiiinooooouuuuyy'))



{# #FAIRE UN SELECT + JOINTURE #}

Select * FROM {{ ref('geographical_references_regions') }} 
Select * FROM {{ ref('education_moyenne_eleve_region') }} AS D



WITH geo_regions AS (
    SELECT 
        code,
        UPPER(TRANSLATE(nom, 'àâäáãåçéèêëíìîïñóòôöõúùûüýÿ', 'aaaaaaceeeeiiiinooooouuuuyy')) AS commune
    FROM {{ ref('geographical_references_regions') }}
),

education_regions AS (
    SELECT 
        id,
        NULL AS code_postal,
        NULL AS commune,
        NULL AS departement,
        NULL AS academie,
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
        created_at
    FROM {{ ref('education_moyenne_eleve_region') }}
)



{# #FAIRE UN SELECT + JOINTURE #}

{# PUIS UN UNION ALL DES 3 TABLES #}

union_c_d_r AS (
    SELECT 
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
        created_at
    FROM {{ ref('education_moyenne_eleve_commune') }}

    UNION ALL

    SELECT 
        id,
        NULL AS code_postal,
        NULL AS commune,
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
        created_at
    FROM {{ ref('education_moyenne_eleve_departement') }}

    UNION ALL

    SELECT 
        id,
        NULL AS code_postal,
        NULL AS commune,
        NULL AS departement,
        NULL AS academie,
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
        created_at
    FROM {{ ref('education_moyenne_eleve_region') }}
)

SELECT 
    G.code AS code_insee,
    G.region_code AS region_code,
    F.id,
    F.code_postal,
    F.commune,
    F.departement,
    F.academie,
    F.region_academique,
    F.rentree_scolaire,
    F.nombre_total_eleves,
    F.nombre_total_classes,
    F.nombre_eleves_ulis,
    F.nombre_eleves_cp_hors_ulis,
    F.nombre_eleves_ce1_hors_ulis,
    F.nombre_eleves_ce2_hors_ulis,
    F.nombre_eleves_cm1_hors_ulis,
    F.nombre_eleves_cm2_hors_ulis,
    F.nombre_eleves_elementaire_hors_ulis,
    F.nombre_eleves_preelementaire_hors_ulis,
    F.created_at
FROM union_c_d_r AS F
LEFT JOIN geo AS G
    ON G.commune = UPPER(TRANSLATE(F.commune, 'àâäáãåçéèêëíìîïñóòôöõúùûüýÿ', 'aaaaaaceeeeiiiinooooouuuuyy'))