{{ config(
    tags = ['silver', 'homepage'],
    alias = 'silver_homepage_accueil'
) }}

with accueil as (
    select
        id,
        latitude::float as acc_latitude,
        longitude::float as acc_longitude,
        "type",
        SPLIT_PART(places, '.', 1)::INTEGER as places
    from {{ ref('homepage_accueil') }}
),
geopoint_code_commune as (
    select
        latitude::float as com_latitude,
        longitude::float as com_longitude,
        code_officiel_commune
    from {{ ref('geographical_references_geopoint_communes') }}
),
distances as (
    select
        a.id,
        a.places,
        a.type,
        a.acc_latitude,
        a.acc_longitude,
        g.code_officiel_commune,
        2 * 6371 * asin(sqrt(
            power(sin(radians((g.com_latitude - a.acc_latitude) / 2)), 2) +
            cos(radians(a.acc_latitude)) * cos(radians(g.com_latitude)) *
            power(sin(radians((g.com_longitude - a.acc_longitude) / 2)), 2)
        )) as distance_km
    from accueil a
    cross join geopoint_code_commune g
),
min_distances as (
    select
        id,
        code_officiel_commune,
        places,
        type,
        distance_km,
        row_number() over (partition by id order by distance_km) as distance_rank
    from distances
),
filtered_table as (
    select
        id,
        code_officiel_commune,
        type,
        places
    from min_distances
    where distance_rank = 1
)

select * from filtered_table