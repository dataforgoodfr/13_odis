{{ config(
    tags = ['gold', 'presentation','elu'],
    alias = 'gold_presentation_elus'
    )
}}

with renommage as (
    select
        case
            when libelle_de_la_fonction = 'Maire' 
                then com_code
            when libelle_de_la_fonction = 'Président du conseil départemental' 
                then dep_code 
            when libelle_de_la_fonction = 'Président du conseil régional' 
                then concat('reg', reg_code)
        end as codgeo,
        prenom_de_l_elu as prenom,
        nom_de_l_elu as nom,
        libelle_de_la_fonction as fonction,
        case
            when libelle_nuance is null
                then 'Non connu'
            else libelle_nuance
        end as libelle
    from {{ ref ("silver_presentation_elus") }} 
)

select * from renommage
