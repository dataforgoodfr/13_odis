{{ config(
    tags = ['bronze', 'presentation'],
    alias = 'vw_presentation_elus_communes'
    )
}}

select 
    "id",
  "prenom_de_l_elu",
  "nom_de_l_elu",
  "date_de_naissance",
  "libelle_de_la_fonction",
  "filename",
  "date_de_debut_du_mandat",
  "date_de_debut_de_la_fonction",
  "libelle_de_la_categorie_socio_professionnelle",
  "code_de_la_categorie_socio_professionnelle",
  "code_nationalite",
  "code_sexe",
  "reg_name",
  "reg_code",
  "dep_name",
  "dep_code",
  "libelle_de_la_section_departementale",
  "code_de_la_section_departementale",
  "epci_name",
  "epci_code",
  "com_name",
  "com_code",
  "libelle_du_canton",
  "code_du_canton",
  "libelle_de_la_circonscription_metropolitaine",
  "code_de_la_circonscription_metropolitaine",
  "libelle_de_la_collectivite_a_statut_particulier",
  "code_de_la_collectivite_a_statut_particulier",
  "created_at"
from {{ source('bronze', 'presentation_elus_communes') }} 
