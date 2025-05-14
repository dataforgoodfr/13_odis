{{ config(
    tags = ['bronze', 'education'],
    alias = 'vw_education_moyenne_eleve_departement'
    )
}}


with eleve_departement as 
(
    select 
        id, 
        (data::jsonb) ->> 'departement' as departement,
        (data::jsonb) ->> 'academie' as academie,
        (data::jsonb) ->> 'region_academique' as region_academique,
        (data::jsonb) ->> 'rentree_scolaire' as rentree_scolaire,
        (data::jsonb) ->> 'sum(nombre_total_eleves)' as nombre_total_eleves,
        (data::jsonb) ->> 'sum(nombre_total_classes)' as nombre_total_classes,
        (data::jsonb) ->> 'sum(nombre_eleves_ulis)' as nombre_eleves_ulis,
        (data::jsonb) ->> 'sum(nombre_eleves_cp_hors_ulis)' as nombre_eleves_cp_hors_ulis,
        (data::jsonb) ->> 'sum(nombre_eleves_ce1_hors_ulis)' as nombre_eleves_ce1_hors_ulis,
        (data::jsonb) ->> 'sum(nombre_eleves_ce2_hors_ulis)' as nombre_eleves_ce2_hors_ulis,
        (data::jsonb) ->> 'sum(nombre_eleves_cm1_hors_ulis)' as nombre_eleves_cm1_hors_ulis,
        (data::jsonb) ->> 'sum(nombre_eleves_cm2_hors_ulis)' as nombre_eleves_cm2_hors_ulis,
        (data::jsonb) ->> 'sum(nombre_eleves_elementaire_hors_ulis)' as nombre_eleves_elementaire_hors_ulis,
        (data::jsonb) ->> 'sum(nombre_eleves_preelementaire_hors_ulis)' as nombre_eleves_preelementaire_hors_ulis,
        created_at

    from {{ source('bronze', 'education_moyenne_eleve_departement') }}  
)

select * from eleve_departement
