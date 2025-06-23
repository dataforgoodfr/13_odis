{{ config(
    tags = ['silver', 'emploi'],
    alias = 'vw_silver_emploi_demandeur_emploi',
    materialized = 'view'
    )
}}

with commune as (
    select 
        *,
        'commune' as type_geo,
        right(zone_geo, 5) as code_postal,
        left(zone_geo, length(zone_geo) - 6) as nom,
        regexp_replace(
            upper(translate(
                left(zone_geo, length(zone_geo) - 6),
                'àâäáãåçéèêëíìîïñóòôöõúùûüýÿÀÂÄÁÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕÚÙÛÜÝŸ',
                'aaaaaaceeeeiiiinooooouuuuyyAAAAAACEEEEIIIINOOOOOUUUUYY'
            )),
            '[-'']', ' ', 'g'
        ) as intitule
    from {{ ref('emploi_demandeur_emploi_communes') }}
), 

geo_commune as(
    select
        "Code_commune_INSEE" as code_geo,
        "Nom_de_la_commune" as nom,
        "Code_postal" as code_postal,
        "Intitule" as commune
    from {{ ref('corresp_codes_communes') }}
    group by 
        code_geo,
        nom,
        code_postal,
        commune
),

join_communes as ( 
    select 
        c.*,
        gc.code_geo as code_geo
    from commune c    
        left join geo_commune gc
        on c.code_postal = gc.code_postal
        and c.intitule like gc.commune
),

departements as 
(
    select 
        *,
        'departement' as type_geo,
        replace(right(zone_geo, 3), ' ', '') as code_postal,
        left(zone_geo, length(zone_geo) - 3) as nom,
        regexp_replace(
            upper(translate(
                left(zone_geo, length(zone_geo) - 3),
                'àâäáãåçéèêëíìîïñóòôöõúùûüýÿÀÂÄÁÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕÚÙÛÜÝŸ',
                'aaaaaaceeeeiiiinooooouuuuyyAAAAAACEEEEIIIINOOOOOUUUUYY'
            )),
            '[-'']', ' ', 'g'
        ) as intitule,
        replace(right(zone_geo, 3), ' ', '') as code_geo
    from {{ ref('emploi_demandeur_emploi_departements') }} 
),

regions as 
(
    select 
        r.*,
        'region' as type_geo,
        gr.code as code_postal,
        replace(r.zone_geo, ' ', '-') as nom,
        regexp_replace(
            upper(translate(
                zone_geo,
                'àâäáãåçéèêëíìîïñóòôöõúùûüýÿÀÂÄÁÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕÚÙÛÜÝŸ',
                'aaaaaaceeeeiiiinooooouuuuyyAAAAAACEEEEIIIINOOOOOUUUUYY'
            )),
            '[-'']', ' ', 'g'
        ) as intitule,
        gr.code as code_geo
    from {{ ref('emploi_demandeur_emploi_regions') }} r
        left join {{ ref('geographical_references_regions') }} gr
    on replace(r.zone_geo, ' ', '-') = gr.intitule 
)

select * from join_communes
    union all
select * from departements
    union all
select * from regions

