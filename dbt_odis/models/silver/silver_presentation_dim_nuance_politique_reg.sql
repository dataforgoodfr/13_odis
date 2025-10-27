with unique_code_nuance as (
    select distinct
        (regexp_match(
     nom_tete_de_liste,
     '^([[:upper:]\-\'']+(?:\s+[[:upper:]\-\'']+)*)\s+(.+)$'
  ))[1] AS nom,
		(regexp_match(
     nom_tete_de_liste,
     '^([[:upper:]\-\'']+(?:\s+[[:upper:]\-\'']+)*)\s+(.+)$'
  ))[2] AS prenom,
        reg_code as code_officiel_region,
        nuance_liste as code_nuance
    from "odis"."bronze"."vw_presentation_elections_regionales"
),
nuance_libelle as (
    select
        unique_code_nuance.nom,
        unique_code_nuance.prenom,
        unique_code_nuance.code_officiel_region,
        unique_code_nuance.code_nuance,
        corresp_codes_nuances.libelle as libelle_nuance
    from unique_code_nuance
    left join "odis"."bronze"."corresp_codes_nuances" as corresp_codes_nuances
        on unique_code_nuance.code_nuance = corresp_codes_nuances.code_nuance
)

select * from nuance_libelle
