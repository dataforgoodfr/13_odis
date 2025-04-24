{{ config(
    alias = 'silver_emploi_demandeur_emploi'
    )
}}

with communes as 
(
    select 
        *,
        'commune' as type_geo,
        right(commune, 5) as code_geo,
        left(commune, length(commune) - 6) as nom 
    from {{ ref('emploi_demandeur_emploi_communes') }}  
),

departements as 
(
    select 
        *,
        'departement' as type_geo,
        replace(right(departement, 3), ' ', '') as code_geo,
        left(departement, length(departement) - 3) as nom 
    from {{ ref('emploi_demandeur_emploi_departements') }} 
),

regions as 
(
    select 
        r.*,
        'region' as type_geo,
        gr.code as code_geo,
        replace(r.region, ' ', '-') as nom
    from {{ ref('emploi_demandeur_emploi_regions') }} r
        left join {{ ref('geographical_references_regions') }} gr
    on replace(r.region, ' ', '-') = gr.intitule 
)

select * from communes
    union all
select * from departements
    union all
select * from regions

