{{ config(
    tags = ['bronze', 'geographical_references'],
    alias = 'vw_geographical_references_geopoint_communes'
    )
}}


with geopoint_communes as 
(
    select 
        "a" as id,
        "GeoPoint" as GeoPoint,
        "Annee" as Annee,
        "Code_Officiel_Commune" as Code_Officiel_Commune,
        "latitude" as Latitude,
        "longitude" as Longitude
    from {{ source('bronze', 'corresp_geo_point_codes_communes') }} 
)

select * from geopoint_communes
