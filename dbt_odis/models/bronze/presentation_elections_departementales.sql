{{ config(
    tags = ['bronze', 'presentation'],
    alias = 'vw_presentation_elections_departementales'
    )
}}

select 
    "index",
  "code_du_departement",
  "libelle_du_departement",
  "code_canton",
  "libelle_canton",
  "n_panneau",
  "n_depot",
  "nuance_binome",
  "n_ordre_candidat",
  "sexe_candidat",
  "nom_candidat",
  "prenom_candidat",
  "nom_nais_candidat",
  "prenom_nais_candidat",
  "date_naissance_candidat",
  "profession_candidat",
  "sexe_supp",
  "nom_supp",
  "prenom_supp",
  "nom_nais_supp",
  "prenom_nais_supp",
  "date_naiss_supp"
from {{ source('bronze', 'presentation_elections_departementales') }} 
