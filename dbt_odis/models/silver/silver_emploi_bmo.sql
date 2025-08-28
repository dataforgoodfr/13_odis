{{ config(
    tags = ['silver', 'emploi'],
    alias = 'silver_emploi_bmo'
) }}

with codes as (
    select
        "Code_FAP228",
        "Intitulé_FAP228",
        "Code_FAP86",
        "Intitulé_FAP86",
        "Code_FAP22",
        "Intitulé_FAP22"
    from {{ source('bronze', 'Intitulés_FAP2021') }}
    group by
        "Code_FAP228",
        "Intitulé_FAP228",
        "Code_FAP86",
        "Intitulé_FAP86",
        "Code_FAP22",
        "Intitulé_FAP22"

),
bmo as (
    select
        extract(year from cast(b."annee" || '-01-01' as date)) as "YEAR",
        b."Code métier BMO",
        b."Nom métier BMO",
        b."Famille_met",
        b."Lbl_fam_met",
        f."Code_FAP22",
        f."Intitulé_FAP22",
        lpad(b."REG" ::text, 2, '0') as "REG",
        b."NOM_REG",
        b."Dept",
        b."NomDept",
        lpad(b."BE24" ::text, 4, '0') as "BE24",
        b."NOMBE24",
        case
            when b."met" = '*' then 1
            else b."met"::integer
        end as "met",
        case
            when b."xmet" = '*' then 1
            else b."xmet"::integer
        end as "xmet",
        case
            when b."smet" = '*' then 1
            else b."smet"::integer
        end as "smet"
    from {{ ref('emploi_bmo_2024') }} b
        left join codes f
        on b."Code métier BMO" = f."Code_FAP228"
),

communes_bassins as (
    select
        lpad(reg, 2, '0') as "reg",
        lpad(dep, 2, '0') as "dep",
        code_commune_insee,
        lpad("code_bassin_BMO"::text, 4, '0') as "code_bassin_BMO",
        "lib_bassin_BMO"
    from {{ ref('bassin_emploi') }}
),

join_communes as (
    select
        bmo."YEAR",
        bmo."Code métier BMO",
        bmo."Nom métier BMO",
        bmo."Famille_met",
        bmo."Lbl_fam_met",
        bmo."Code_FAP22",
        bmo."Intitulé_FAP22",
        bmo."REG",
        bmo."NOM_REG",
        bmo."Dept",
        bmo."NomDept",
        bmo."BE24",
        bmo."NOMBE24",
        bmo."met",
        bmo."xmet",
        bmo."smet",
        cb."code_commune_insee" as code_geo,
        'commune' as type_geo
    from bmo
    right join communes_bassins cb
    on left(bmo."Dept"::text, 2) = cb."dep"
    and bmo."BE24" = cb."code_bassin_BMO"
    and bmo."REG" = cb."reg"

),

sum_dept as (
    select
        "YEAR",
        "Code métier BMO",
        "Nom métier BMO",
        "Famille_met",
        "Lbl_fam_met",
        "Code_FAP22",
        "Intitulé_FAP22",
        "REG",
        "NOM_REG",
        "Dept",
        "NomDept",
        null as "BE24",
        null as "NOMBE24",
        sum(met) as "met",
        sum(xmet) as "xmet",
        sum(smet) as "smet",
        "Dept" as code_geo,
        'département' as type_geo
    from bmo
    group by
        "YEAR",
        "Code métier BMO",
        "Nom métier BMO",
        "Famille_met",
        "Lbl_fam_met",
        "Code_FAP22",
        "Intitulé_FAP22",
        "REG",
        "NOM_REG",
        "Dept",
        "NomDept"
),

sum_reg as (
    select
        "YEAR",
        "Code métier BMO",
        "Nom métier BMO",
        "Famille_met",
        "Lbl_fam_met",
        "Code_FAP22",
        "Intitulé_FAP22",
        "REG",
        "NOM_REG",
        null as "Dept",
        null as "NomDept",
        null as "BE24",
        null as "NOMBE24",
        sum(met) as "met",
        sum(xmet) as "xmet",
        sum(smet) as "smet",
        concat('reg', "REG") as code_geo,
        'région' as type_geo
    from bmo
    group by
        "YEAR",
        "Code métier BMO",
        "Nom métier BMO",
        "Famille_met",
        "Lbl_fam_met",
        "Code_FAP22",
        "Intitulé_FAP22",
        "REG",
        "NOM_REG"
)

select * from join_communes
union all
select * from sum_dept
union all
select * from sum_reg