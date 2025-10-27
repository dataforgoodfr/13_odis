with unique_code_nuance as (
    select distinct
        nom_candidat as nom,
        prenom_candidat as prenom,
        code_du_departement as code_officiel_du_departement,
        nuance_binome as code_nuance
    from {{ ref('presentation_elections_departementales') }}  
),
nuance_libelle as (
    select
        unique_code_nuance.nom,
        unique_code_nuance.prenom,
        unique_code_nuance.code_officiel_du_departement,
        unique_code_nuance.code_nuance,
        corresp_codes_nuances.libelle as libelle_nuance
    from unique_code_nuance
    left join {{ ref('corresp_codes_nuances') }} as corresp_codes_nuances
        on unique_code_nuance.code_nuance = corresp_codes_nuances.code_nuance
    --where unique_code_nuance.rank_=1
)

select * from nuance_libelle
