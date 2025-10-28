{{ config(
    tags = ['bronze', 'presentation'],
    alias = 'vw_presentation_elections_municipales'
    )
}}

select 
    "id",
  "code_officiel_departement",
  "nom_officiel_departement",
  "nom_officiel_commune",
  "code_bvote",
  "inscrits",
  "abstentions",
  "absins",
  "votants",
  "votins",
  "blancs",
  "blancsins",
  "blancsvot",
  "nuls",
  "nulsins",
  "nulsvot",
  "exprimes",
  "expins",
  "expvot",
  "npan",
  "code_nuance",
  "sexe",
  "nom",
  "prenom",
  "liste",
  "voix",
  "voixins",
  "voixexp",
  "nom_prenom",
  "code_officiel_commune",
  "localisation",
  "nom_bureau",
  "code_officiel_region",
  "nom_officiel_region",
  "code_iso_31663_zone",
  "created_at"
from {{ source('bronze', 'presentation_elections_municipales') }} 
