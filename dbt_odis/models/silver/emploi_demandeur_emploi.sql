{{ config(
    tags = ['silver', 'emploi'],
    alias = 'silver_emploi_demandeur_emploi'
    )
}}

with communes as 
(
    select 
        *,
        'commune' as type_geo,
        right(zone_geo, 5) as code_geo,
        left(zone_geo, length(zone_geo) - 6) as nom 
    from {{ ref('emploi_demandeur_emploi_communes') }}  
),

departements as 
(
    select 
        *,
        'departement' as type_geo,
        replace(right(zone_geo, 3), ' ', '') as code_geo,
        left(zone_geo, length(zone_geo) - 3) as nom 
    from {{ ref('emploi_demandeur_emploi_departements') }} 
),

regions as 
(
    select 
        r.*,
        'region' as type_geo,
        gr.code as code_geo,
        replace(r.zone_geo, ' ', '-') as nom
    from {{ ref('emploi_demandeur_emploi_regions') }} r
        left join {{ ref('geographical_references_regions') }} gr
    on replace(r.zone_geo, ' ', '-') = gr.intitule 
)

select * from communes
    union all
select * from departements
    union all
select * from regions

