{{ config(
    alias = 'silver_education_moyenne_eleve'
    )
}}



Select * FROM {{ ref('geographical_references_communes') }} 
Select * FROM {{ ref('education_moyenne_eleve_commune') }} AS B ORDER BY commune ASC
Select * FROM {{ ref('education_moyenne_eleve_departement') }} AS C
Select * FROM {{ ref('education_moyenne_eleve_region') }} AS D



WITH geo AS (
    SELECT 
        code,
        region_code,
        UPPER(TRANSLATE(nom, 'àâäáãåçéèêëíìîïñóòôöõúùûüýÿ', 'aaaaaaceeeeiiiinooooouuuuyy')) AS commune
    FROM {{ ref('geographical_references_communes') }}
),

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