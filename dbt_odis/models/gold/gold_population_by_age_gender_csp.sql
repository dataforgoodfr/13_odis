{{ config(
    tags=['gold', 'population'],
    alias='vw_population_by_age_gender_csp'
) }}

with code_geo_final as (
    select
        case
            when A.geocode_type = 'REG' then concat('reg', A.geocode)
            else A.geocode end
        as codgeo,
        A.time_period as "year",
        A.POP,
        A.POPH,
        A.H0015,
        A.H1524,
        A.H2539,
        A.H4054,
        A.H5564,
        A.H6579,
        A.H80P,
        A.POPF,
        A.F0015,
        A.F1524,
        A.F2539,
        A.F4054,
        A.F5564,
        A.F6579,
        A.F80P,
        B.POP15P,
        B.POP15P_CS1,
        B.POP15P_CS2,
        B.POP15P_CS3,
        B.POP15P_CS4,
        B.POP15P_CS5,
        B.POP15P_CS6,
        B.POP15P_CS7,
        B.POP15P_CS9
        --H7589_H90P, --in expected table but missing in the data
        --F7589_F90P --in expected table but missing in the data
    from {{ ref('stg_population_by_age_gender') }} A
    join {{ ref('stg_population_categorie_socio_pro') }} B
    on A.geocode = B.geocode
    and A.geocode_type = B.geocode_type
    and A.geocode_year = B.geocode_year
    and A.time_period = B.time_period
)

select * from code_geo_final
