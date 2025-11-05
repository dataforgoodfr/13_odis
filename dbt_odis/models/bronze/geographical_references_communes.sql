{{ config(
    tags = ['bronze', 'geographical_references'],
    alias = 'vw_geographical_references_communes'
    )
}}


with communes as 
(
    select 
        id, 
        (data::jsonb) ->> 'nom' as nom, 
        (data::jsonb) ->> 'code' as code, 
        (data::jsonb) -> 'centre' ->> 'type' as geo_type,
        (data::jsonb) -> 'centre' -> 'coordinates'->> 0 as geo_coordonnees_longitude,
        (data::jsonb) -> 'centre' -> 'coordinates'->> 1 as geo_coordonnees_lattitude,
        (data::jsonb) -> 'region' ->> 'nom' as region_nom,
        (data::jsonb) -> 'region' ->> 'code' as region_code,
        (data::jsonb) -> 'departement' ->> 'nom' as departement_nom,
        (data::jsonb) -> 'departement' ->> 'code' as departement_code,
        (data::jsonb) -> 'population' as population,
        (data::jsonb) -> 'epci' ->> 'code' as epci_code,
        (data::jsonb) -> 'epci' ->> 'nom' as epci_nom,
        created_at
    from {{ source('bronze', 'geographical_references_communes') }} 
)

select * from communes
