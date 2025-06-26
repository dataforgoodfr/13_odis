{{ config(
    tags = ['silver', 'emploi'],
    alias = 'emploi_bmo_avec_communes'
) }}

with
    bmo as (
        select
            "annee",
            "Code métier BMO",
            "Nom métier BMO",
            "Famille_met",
            "Lbl_fam_met",
            "REG",
            "NOM_REG",
            "Dept",
            "NomDept",
            "BE24",
            "NOMBE24",
            case
                when met = '*' then 1
                else met::integer
            end as "met",
            case
                when xmet = '*' then 1
                else xmet::integer
            end as "xmet",
            case
                when smet = '*' then 1
                else smet::integer
            end as "smet"
        from {{ source('bronze', 'vw_emploi_bmo_2024') }}
    ),
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
            sum(met) as "met",
            sum(xmet) as "xmet",
            sum(smet) as "smet"
        from bmo
        group by
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
        select distinct
            "Dept",
            "NomDept"
        from bmo
    ),
    bassins_communes as (
        select
            "dep",
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
        from  bassins_communes
        left join bmo_sum_dept
        on bassins_communes."code_bassin_BMO" = bmo_sum_dept."BE24"
        left join departements
        on bassins_communes."dep" = departements."Dept"
    )
select * from bmo_be_sum_communes