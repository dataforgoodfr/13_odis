{{ config(
    tags=['silver', 'population'],
    alias='silver_population_by_age_gender'
) }}

with

women_raw as (
    SELECT
        dimensions_geo,
        dimensions_time_period,
        dimensions_age,
        measures_obs_value_niveau_value::NUMERIC as pop
    from  {{ ref('population_by_age_gender')}}
    where dimensions_sex = 'F' and dimensions_age != '_T' and dimensions_age != 'Y_LT20' 
    and dimensions_age != 'Y_GE75' and dimensions_age != 'Y20T39' and dimensions_age != 'Y40T59'
    and dimensions_age != 'Y60T74'
),
women_age_class_refined as (
    select
        dimensions_geo,
        dimensions_time_period,
        sum(case when dimensions_age in ('Y_LT5', 'Y5T9', 'Y10T14') then pop end) as F0014,
        sum(case when dimensions_age in ('Y15T19', 'Y20T24', 'Y25T29') then pop end) as F1529,
        sum(case when dimensions_age in ('Y30T34', 'Y35T39', 'Y40T44') then pop end) as F3044,
        sum(case when dimensions_age in ('Y45T49', 'Y50T54', 'Y55T59') then pop end) as F4559,
        sum(case when dimensions_age in ('Y60T64', 'Y65T69', 'Y70T74') then pop end) as F6074,
        sum(case when dimensions_age in ('Y75T79', 'Y80T84', 'Y85T89') then pop end) as F7589,
        sum(case when dimensions_age in ('Y90T94', 'Y_GE95') then pop end) as F90P
    from women_raw
    group by dimensions_geo, dimensions_time_period
),
women_final as (
    SELECT
        dimensions_geo as codegeo,
        dimensions_time_period as year_,
        F0014 + F1529 + F3044 + F4559 + F6074 + F7589 + F90P as popf,
        F0014,
        F1529,
        F3044,
        F4559,
        F6074,
        F7589,
        F90P
    from women_age_class_refined
),
men_raw as (
    SELECT
        dimensions_geo,
        dimensions_time_period,
        dimensions_age,
        measures_obs_value_niveau_value::NUMERIC as pop
    from  {{ ref('population_by_age_gender')}}
    where dimensions_sex = 'M' and dimensions_age != '_T' and dimensions_age != 'Y_LT20' 
    and dimensions_age != 'Y_GE75' and dimensions_age != 'Y20T39' and dimensions_age != 'Y40T59'
    and dimensions_age != 'Y60T74'
),
men_age_class_refined as (
    select
        dimensions_geo,
        dimensions_time_period,
        sum(case when dimensions_age in ('Y_LT5', 'Y5T9', 'Y10T14') then pop end) as H0014,
        sum(case when dimensions_age in ('Y15T19', 'Y20T24', 'Y25T29') then pop end) as H1529,
        sum(case when dimensions_age in ('Y30T34', 'Y35T39', 'Y40T44') then pop end) as H3044,
        sum(case when dimensions_age in ('Y45T49', 'Y50T54', 'Y55T59') then pop end) as H4559,
        sum(case when dimensions_age in ('Y60T64', 'Y65T69', 'Y70T74') then pop end) as H6074,
        sum(case when dimensions_age in ('Y75T79', 'Y80T84', 'Y85T89') then pop end) as H7589,
        sum(case when dimensions_age in ('Y90T94', 'Y_GE95') then pop end) as H90P
    from men_raw
    group by dimensions_geo, dimensions_time_period
),
men_final as (
    SELECT
        dimensions_geo as codegeo,
        dimensions_time_period as year_,
        H0014 + H1529 + H3044 + H4559 + H6074 + H7589 + H90P as popH,
        H0014,
        H1529,
        H3044,
        H4559,
        H6074,
        H7589,
        H90P
    from men_age_class_refined
),
all_gender as (
    SELECT
        w.codegeo,
        w.year_ as "year",
        m.poph + w.popf as pop,
        m.poph,
        m.H0014,
        m.H1529,
        m.H3044,
        m.H4559,
        m.H6074,
        m.H7589,
        m.H90P,
        w.popf,
        w.F0014,
        w.F1529,
        w.F3044,
        w.F4559,
        w.F6074,
        w.F7589,
        w.F90P
    from women_final w
    join men_final m on m.codegeo = w.codegeo AND m.year_ = w.year_
)

select * from all_gender