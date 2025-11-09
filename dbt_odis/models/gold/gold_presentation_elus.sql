{{ config(
    tags = ['gold', 'presentation','elu'],
    )
}}

with renommage as (
    select
        codgeo,
        prenom_de_l_elu as prenom,
        nom_de_l_elu as nom,
        libelle_de_la_fonction as fonction,
        libelle_nuance as libelle
    from {{ ref ("silver_presentation_elus") }} 
)

select * from renommage
