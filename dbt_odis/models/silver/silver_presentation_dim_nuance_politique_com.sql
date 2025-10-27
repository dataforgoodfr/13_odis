{{ config(
    tags = ['silver', 'presentation','elu'],
    )
}}

with unique_code_nuance as (
    select distinct
        nom,
        prenom,
        code_officiel_commune,
        code_nuance
        --row_number() over(partition by nom,prenom order by id desc) rank_
    from {{ ref ("presentation_elections_municipales") }}
),

nuance_libelle as (
    select
        unique_code_nuance.nom,
        unique_code_nuance.prenom,
        unique_code_nuance.code_officiel_commune,
        unique_code_nuance.code_nuance,
        corresp_codes_nuances.libelle as libelle_nuance
    from unique_code_nuance
    left join {{ ref('corresp_codes_nuances') }} as corresp_codes_nuances
        on unique_code_nuance.code_nuance = corresp_codes_nuances.code_nuance
    --where unique_code_nuance.rank_=1
)

select * from nuance_libelle