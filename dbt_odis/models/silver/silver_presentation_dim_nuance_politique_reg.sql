{{ config(
    tags = ['silver', 'presentation','elu'],
    alias = 'vw_silver_presentation_dim_nuance_politique_reg',
    materialized = 'view'
    )
}}

with unique_code_nuance as (
    select distinct
        {{ split_last_and_first_name('nom_tete_de_liste') }},
        reg_code as code_officiel_region,
        nuance_liste as code_nuance
    from {{ ref('presentation_elections_regionales') }}
),
nuance_libelle as (
    select
        unique_code_nuance.nom,
        unique_code_nuance.prenom,
        code_officiel_region,
        unique_code_nuance.code_nuance,
        corresp_codes_nuances.libelle as libelle_nuance
    from unique_code_nuance
    left join {{ ref('corresp_codes_nuances') }} as corresp_codes_nuances
        on unique_code_nuance.code_nuance = corresp_codes_nuances.code_nuance
)

select * from nuance_libelle
