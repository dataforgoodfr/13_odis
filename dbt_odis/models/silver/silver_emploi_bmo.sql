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

)
    select
        b."annee",
        b."Code métier BMO",
        b."Nom métier BMO",
        b."Famille_met",
        b."Lbl_fam_met",
        f."Code_FAP22",
        f."Intitulé_FAP22",
        b."REG" ::text,
        b."NOM_REG",
        b."Dept",
        b."NomDept",
        b."BE24" ::text,
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
{# ),
    bmo_sum_dept as (
        select
            "annee",
            "Code métier BMO",
            "Nom métier BMO",
            "Famille_met",
            "Lbl_fam_met",
            "REG",
            "NOM_REG",
            "BE24",
            "NOMBE24",
            SUM(met) as "met",
            SUM(xmet) as "xmet",
            SUM(smet) as "smet"
        from bmo
        GROUP BY
            "annee",
            "Code métier BMO",
            "Nom métier BMO",
            "Famille_met",
            "Lbl_fam_met",
            "REG",
            "NOM_REG",
            "BE24",
            "NOMBE24"
    ),
    departements as (
        select DISTINCT
            "Dept",
            "NomDept"
        from bmo
    ),
    bassins_communes as (
        select
            LPAD("dep"::text, 2, '0') as "dep",
            "code_commune_insee",
            "code_bassin_BMO"
        from {{ ref('bassin_emploi') }}
    ),
    bmo_be_sum_communes as (
        select
            bmo_sum_dept."annee",
            bmo_sum_dept."Code métier BMO",
            bmo_sum_dept."Nom métier BMO",
            bmo_sum_dept."Famille_met",
            bmo_sum_dept."Lbl_fam_met",
            bmo_sum_dept."REG",
            bmo_sum_dept."NOM_REG",
            departements."Dept",
            departements."NomDept",
            bassins_communes."code_commune_insee",
            bmo_sum_dept."BE24",
            bmo_sum_dept."NOMBE24",
            bmo_sum_dept."met",
            bmo_sum_dept."xmet",
            bmo_sum_dept."smet"
        from bassins_communes
        LEFT JOIN bmo_sum_dept
            ON bassins_communes."code_bassin_BMO" = bmo_sum_dept."BE24"
        LEFT JOIN departements
            ON bassins_communes."dep" = departements."Dept"
    )
select * from bmo_be_sum_communes #}