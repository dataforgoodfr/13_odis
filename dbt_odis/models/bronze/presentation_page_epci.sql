{{ config(
    alias = 'vw_presentation_page_epci'
    )
}}


with epci as 
(
    select 
        id as id, 
        json_value(data, '$.com_code') as com_code, 
        json_value(data, '$.com_name') as com_name, 
        json_value(data, '$.dep_code') as dep_code,
        json_value(data, '$.dep_name') as dep_name,
        json_value(data, '$.filename') as filename,
        json_value(data, '$.reg_code') as reg_code,
        json_value(data, '$.reg_name') as reg_name,
        json_value(data, '$.code_sexe') as code_sexe,
        json_value(data, '$.epci_code') as epci_code,
        json_value(data, '$.epci_name') as epci_name,
        json_value(data, '$.code_du_canton') as code_canton,
        json_value(data, '$.libelle_du_canton') as libelle_canton,
        json_value(data, '$.nom_de_l_elu') as nom_elu,      
        json_value(data, '$.prenom_de_l_elu') as prenom_elu,
        json_value(data, '$.code_nationalite') as code_nationalite,
        json_value(data, '$.libelle_de_la_fonction') as libelle_fonction,
        json_value(data, '$.date_de_debut_du_mandat') as date_debut_mandat,
        json_value(data, '$.date_de_debut_de_la_fontcion') as date_debut_fontion,
        json_value(data, '$.code_de_la_section_departementale') as code_section_departementale,
        json_value(data, '$.libelle_de_la_section_departementale') as libelle_section_departementale,
        json_value(data, '$.code_de_la_circonscription_metropolitaine') as code_circonscription,
        json_value(data, '$.libelle_de_la_circonscription_metropolitaine') as libelle_circonscription,
        json_value(data, '$.code_de_la_categorie_socio_professionnelle') as code_csp,
        json_value(data, '$.libelle_de_la_categorie_socio_professionnelle') as libelle_csp,
        json_value(data, '$.code_de_la_collectivite_a_statut_particulier') as code_collectivite_particulier,
        json_value(data, '$.libelle_de_la_collectivite_a_statut_particulier') as libelle_collectivite_particulier,
        created_at as created_at
    from {{ source('bronze', 'presentation_page_epci') }} 
)

select * from epci

