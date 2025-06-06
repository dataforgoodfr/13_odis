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
), geo_commune as(
select
    "Code_commune_INSEE" as code_geo,
    "Nom_de_la_commune" as nom,
    "Code_postal" as code_postal,
    "Libellé_d_acheminement" as commune
from {{ ref('corresp_codes_communes') }}
group by 
    code_geo,
    nom,
    code_postal,
    commune
)
    
select 
    c.*,
    gc.code_geo as code_geo
from commune c    
    left join geo_commune gc
    on c.code_postal = gc.code_postal
    and '%' || c.intitule || '%' like '%' || gc.commune || '%'
