-- Tests that dep rows equal the sum of their commune rows,
-- and that reg rows equal the sum of their dep rows.
-- Returns one row per failing codgeo / check combination.

with gold as (
    select * from {{ ref('gold_homepage_accueil') }}
),

-- Identify levels using unambiguous patterns
-- Commune: purely numeric 5-char code (e.g. '75056')
-- Dep:     purely numeric 2-3 char code (e.g. '75', '971')
-- Reg:     prefixed with 'reg' (e.g. 'reg11')
communes as (
    select * from gold where codgeo ~ '^[0-9]{5}$'
),
dep_rows as (
    select * from gold where codgeo ~ '^[0-9]{2,3}$'
),
reg_rows as (
    select * from gold where codgeo like 'reg%'
),

-- 1. Recompute dep totals from commune rows
dep_from_communes as (
    select
        left(codgeo, 2)  as codgeo,
        sum(cada)        as cada,
        sum(huda)        as huda,
        sum(prahda)      as prahda,
        sum(cph)         as cph,
        sum(caes)        as caes,
        sum(disp_refu)   as disp_refu
    from communes
    group by left(codgeo, 2)
),

-- 2. Recompute reg totals from dep rows
dep_to_reg as (
    select distinct "CODDEP", concat('reg', "CODREG") as codgeo
    from {{ ref('com_dep_reg') }}
),
reg_from_deps as (
    select
        dtr.codgeo,
        sum(d.cada)      as cada,
        sum(d.huda)      as huda,
        sum(d.prahda)    as prahda,
        sum(d.cph)       as cph,
        sum(d.caes)      as caes,
        sum(d.disp_refu) as disp_refu
    from dep_rows d
    join dep_to_reg dtr on d.codgeo = dtr."CODDEP"
    group by dtr.codgeo
),

-- Failures: dep row != sum of communes
dep_check as (
    select
        coalesce(d.codgeo, c.codgeo) as codgeo,
        'dep_vs_communes'             as check_type
    from dep_rows d
    full join dep_from_communes c on d.codgeo = c.codgeo
    where d.cada      is distinct from c.cada
       or d.huda      is distinct from c.huda
       or d.prahda    is distinct from c.prahda
       or d.cph       is distinct from c.cph
       or d.caes      is distinct from c.caes
       or d.disp_refu is distinct from c.disp_refu
       or d.codgeo    is null   -- dep has communes but no dep row
       or c.codgeo    is null   -- dep row exists but no communes
),

-- Failures: reg row != sum of deps
reg_check as (
    select
        coalesce(r.codgeo, rd.codgeo) as codgeo,
        'reg_vs_deps'                  as check_type
    from reg_rows r
    full join reg_from_deps rd on r.codgeo = rd.codgeo
    where r.cada      is distinct from rd.cada
       or r.huda      is distinct from rd.huda
       or r.prahda    is distinct from rd.prahda
       or r.cph       is distinct from rd.cph
       or r.caes      is distinct from rd.caes
       or r.disp_refu is distinct from rd.disp_refu
       or r.codgeo    is null   -- reg has deps but no reg row
       or rd.codgeo   is null   -- reg row exists but no matching deps
)

select * from dep_check
union all
select * from reg_check
