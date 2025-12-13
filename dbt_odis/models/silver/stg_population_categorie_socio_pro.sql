{{ config(
    tags=['silver', 'population'],
    alias='vw_population_categorie_socio_pro_stg'
) }}

with pop as (
    select
        geo,
        age,
        sex,
        time_period,
        pcs,
        "measure_value"::numeric as measure_value
    from {{ ref('population_categorie_socio_pro') }}
),
pop_csp as (
    select
        geo,
        split_part(geo, '-', 1) as geocode_year,
        split_part(geo, '-', 2) as geocode_type,
        split_part(geo, '-', 3) as geocode,
        time_period,
        sum(case when pcs = '_T' then round(measure_value, 4) end) as POP15P,
        sum(case when pcs = '1' then round(measure_value, 4) end) as POP15P_CS1,
        sum(case when pcs = '2' then round(measure_value, 4) end) as POP15P_CS2,
        sum(case when pcs = '3' then round(measure_value, 4) end) as POP15P_CS3,
        sum(case when pcs = '4' then round(measure_value, 4) end) as POP15P_CS4,
        sum(case when pcs = '5' then round(measure_value, 4) end) as POP15P_CS5,
        sum(case when pcs = '6' then round(measure_value, 4) end) as POP15P_CS6,
        sum(case when pcs = '7' then round(measure_value, 4) end) as POP15P_CS7,
        -- CSP = '8' does not exist for 2022
        sum(case when pcs = '9' then round(measure_value, 4) end) as POP15P_CS9
    from pop
    where age = 'Y_GE15'
    and sex = '_T'
    and time_period = '2022'
    group by
        geo,
        split_part(geo, '-', 1),
        split_part(geo, '-', 2),
        split_part(geo, '-', 3),
        time_period
)

select * from pop_csp


-- cs1: agriculteurs exploitants
-- cs2: artisants, commerçants, chefs entreprise
-- cs3: cadres, professions intellectuelles sup
-- cs4: prof. intermédiaires
-- cs5: employés
-- cs6: ouvriers
-- cs7: retraités
-- cs8: étudiants ou élèves (N/A en 2022)
-- cs9: autres inactifs (nouvelle catégorie)