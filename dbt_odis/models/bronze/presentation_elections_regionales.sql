{{ config(
    tags = ['bronze', 'presentation'],
    alias = 'vw_presentation_elections_regionales'
    )
}}

select 
    "id",
  "dep_code",
  "dep_name",
  "libelle_de_la_section_electorale",
  "com_code",
  "libelle_de_la_commune",
  "code_du_b_vote",
  "inscrits",
  "abstentions",
  "abs_ins",
  "votants",
  "vot_ins",
  "blancs",
  "blancs_ins",
  "blancs_vot",
  "nuls",
  "nuls_ins",
  "nuls_vot",
  "exprimes",
  "exp_ins",
  "exp_vot",
  "ndegliste",
  "nuance_liste",
  "libelle_abrege_liste",
  "libelle_etendu_liste",
  "nom_tete_de_liste",
  "voix",
  "voix_ins",
  "voix_exp",
  "epci_code",
  "epci_name",
  "reg_code",
  "reg_name",
  "location",
  "created_at"
from {{ source('bronze', 'presentation_elections_regionales') }} 
