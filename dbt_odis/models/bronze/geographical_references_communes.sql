{{ config(
    alias = 'vw_geographical_references_communes'
    )
}}


with communes as 
(
    select 
        id as id, 
        json_value(data, '$.nom') as nom, 
        json_value(data, '$.code') as code, 
        json_value(data, '$.centre.type') as geo_type,
        json_value(data, '$.centre.coordinates[0]') as geo_coordonnees_longitude,
        json_value(data, '$.centre.coordinates[1]') as geo_coordonnees_lattitude,
        json_value(data, '$.region.nom') as region_nom,
        json_value(data, '$.region.code') as region_code,
        json_value(data, '$.departement.nom') as departement_nom,
        json_value(data, '$.departement.code') as departement_code,
        json_value(data, '$.population') as population,        
        created_at as created_at
    from {{ source('bronze', 'geographical_references_communes') }} 
)

select * from communes
