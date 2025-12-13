{{ config(
    tags = ['silver', 'population'],
    alias = 'vw_population_by_age_gender_stg'
) }}

with details as (
    select
        geo,
        time_period,
        age,
        sex,
        measure_value::numeric as pop
    from {{ ref('population_by_age_gender') }}
    where age not in ('Y_LT20', 'Y_GE65', 'Y20T64') -- remove unnecessary bins
),
women as (
    select
        geo,
        time_period,
        sex,
        sum(case when age = '_T' then pop end) as POPF,
        sum(case when age = 'Y_LT15' then pop end) as F0015,
        sum(case when age = 'Y15T24' then pop end) as F1524,
        sum(case when age = 'Y25T39' then pop end) as F2539,
        sum(case when age = 'Y40T54' then pop end) as F4054,
        sum(case when age = 'Y55T64' then pop end) as F5564,
        sum(case when age = 'Y65T79' then pop end) as F6579,
        sum(case when age = 'Y_GE80'  then pop end) as F80P
    from details
    where sex = 'F'
    group by geo, time_period, sex
),
men as (
    select
        geo,
        time_period,
        sex,
        sum(case when age = '_T' then pop end) as POPH,
        sum(case when age = 'Y_LT15' then pop end) as H0015,
        sum(case when age = 'Y15T24' then pop end) as H1524,
        sum(case when age = 'Y25T39' then pop end) as H2539,
        sum(case when age = 'Y40T54' then pop end) as H4054,
        sum(case when age = 'Y55T64' then pop end) as H5564,
        sum(case when age = 'Y65T79' then pop end) as H6579,
        sum(case when age = 'Y_GE80'  then pop end) as H80P
    from details
    where sex = 'M'
    group by geo, time_period, sex
),
total as (
    select
        geo,
        time_period,
        sum(case when age = '_T' then pop end) as POP
    from details
    where sex = '_T'
    and age = '_T'
    group by geo, time_period
),
all_age_sex as (
    select
        w.geo,
        split_part(w.geo, '-', 1) as geocode_year,
        split_part(w.geo, '-', 2) as geocode_type,
        split_part(w.geo, '-', 3) as geocode,
        w.time_period,
        sum(t.POP) as POP,
        sum(m.POPH) as POPH,
        sum(m.H0015) as H0015,
        sum(m.H1524) as H1524,
        sum(m.H2539) as H2539,
        sum(m.H4054) as H4054,
        sum(m.H5564) as H5564,
        sum(m.H6579) as H6579,
        sum(m.H80P) as H80P,
        sum(w.POPF) as POPF,
        sum(w.F0015) as F0015,
        sum(w.F1524) as F1524,
        sum(w.F2539) as F2539,
        sum(w.F4054) as F4054,
        sum(w.F5564) as F5564,
        sum(w.F6579) as F6579,
        sum(w.F80P) as F80P
    from women w
    left join men m
    on m.geo = w.geo
    and m.time_period = w.time_period
    left join total t
    on t.geo = w.geo
    and t.time_period = w.time_period
    where w.time_period = '2022'
    group by
        w.geo,
        split_part(w.geo, '-', 1),
        split_part(w.geo, '-', 2),
        split_part(w.geo, '-', 3),
        w.time_period
)

select * from all_age_sex