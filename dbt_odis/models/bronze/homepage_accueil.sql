{{ config(
    tags = ['bronze', 'homepage'],
    alias = 'vw_homepage_accueil'
    )
}}

with accueil as (
    select
        id as id,
        latitude as latitude,
        longitude as longitude,
        gestionnaire as gestionnaire,
        type as type,
        places as places,
        created_at as created_at
    from {{ source('bronze', 'accueil_cada_cph_huda') }}
)

select * from accueil