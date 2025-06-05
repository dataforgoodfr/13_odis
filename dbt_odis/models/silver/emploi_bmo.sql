{{ config(
    materialized='table',
    alias='silver_emploi_bmo_familles_metiers'
    )
}}

select
    vw_emploi_bmo_2024."Famille_met" as code_famille_metier,
    vw_emploi_bmo_2024."Lbl_fam_met" as libelle_famille_metier,
    sum({{ cast_to_integer("case when vw_emploi_bmo_2024.met ~ '^\\s*\\d+\\s*$' then vw_emploi_bmo_2024.met else null end") }}) as nombre_projets_recrutement,
    sum({{ cast_to_integer("case when vw_emploi_bmo_2024.smet ~ '^\\s*\\d+\\s*$' then vw_emploi_bmo_2024.smet else null end") }}) as nombre_projets_recrutement_saisonniers,
    sum({{ cast_to_integer("case when vw_emploi_bmo_2024.xmet ~ '^\\s*\\d+\\s*$' then vw_emploi_bmo_2024.xmet else null end") }}) as nombre_projets_recrutement_difficiles
from {{ ref("emploi_bmo_2024") }} as vw_emploi_bmo_2024
group by vw_emploi_bmo_2024."Famille_met", vw_emploi_bmo_2024."Lbl_fam_met"