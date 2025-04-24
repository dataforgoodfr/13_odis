{{ config(
    alias = 'vw_presentation_page_elus_communes'
    )
}}


with elus_communes as 
(
    select 
        id, 
        (data::jsonb) ->> 'com_code' as com_code, 
        (data::jsonb) ->> 'com_name' as com_name, 
        (data::jsonb) ->> 'dep_code' as dep_code,
        (data::jsonb) ->> 'dep_name' as dep_name,
        (data::jsonb) ->> 'filename' as filename,
        (data::jsonb) ->> 'reg_code' as reg_code,
        (data::jsonb) ->> 'reg_name' as reg_name,
        (data::jsonb) ->> 'code_sexe' as code_sexe,
        (data::jsonb) ->> 'epci_code' as epci_code,
        (data::jsonb) ->> 'epci_name' as epci_name,
        (data::jsonb) ->> 'code_du_canton' as code_canton,
        (data::jsonb) ->> 'libelle_du_canton' as libelle_canton,
        (data::jsonb) ->> 'nom_de_l_elu' as nom_elu,      
        (data::jsonb) ->> 'prenom_de_l_elu' as prenom_elu,
        (data::jsonb) ->> 'code_nationalite' as code_nationalite,
        (data::jsonb) ->> 'libelle_de_la_fonction' as libelle_fonction,
        (data::jsonb) ->> 'date_de_debut_du_mandat' as date_debut_mandat,
        (data::jsonb) ->> 'date_de_debut_de_la_fontcion' as date_debut_fontion,
        (data::jsonb) ->> 'code_de_la_section_departementale' as code_section_departementale,
        (data::jsonb) ->> 'libelle_de_la_section_departementale' as libelle_section_departementale,
        (data::jsonb) ->> 'code_de_la_circonscription_metropolitaine' as code_circonscription,
        (data::jsonb) ->> 'libelle_de_la_circonscription_metropolitaine' as libelle_circonscription,
        (data::jsonb) ->> 'code_de_la_categorie_socio_professionnelle' as code_csp,
        (data::jsonb) ->> 'libelle_de_la_categorie_socio_professionnelle' as libelle_csp,
        (data::jsonb) ->> 'code_de_la_collectivite_a_statut_particulier' as code_collectivite_particulier,
        (data::jsonb) ->> 'libelle_de_la_collectivite_a_statut_particulier' as libelle_collectivite_particulier,
        created_at
    from {{ source('bronze', 'presentation_page_elus_communes') }} 
)

select * from elus_communes
