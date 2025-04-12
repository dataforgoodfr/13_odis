{{ config(
    alias = 'silver_emploi_demandeur_emploi'
    )
}}

with communes as 
(
    select 
        *,
        'commune' as type,
        replace(right(commune, 5), ' ', '') as code,
        replace(left(commune, length(commune) - 5), ' ', '') as nom 
    from {{ ref('emploi_demandeur_emploi_communes') }}  
),

departements as 
(
    select 
        *,
        'departement' as type,
        replace(right(departement, 3), ' ', '') as code,
        replace(left(departement, length(departement) - 3), ' ', '') as nom 
    from {{ ref('emploi_demandeur_emploi_departements') }} 
),

regions as 
(
    select 
        *,
        'region' as type,
        replace(right(region, 2), ' ', '') as code,
        replace(left(region, length(region) - 2), ' ', '') as nom 
    from {{ ref('emploi_demandeur_emploi_regions') }} 
)

select * from communes
    UNION ALL
    select * from departements
    UNION ALL
    select * from regions

 

