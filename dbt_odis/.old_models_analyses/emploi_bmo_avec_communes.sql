{{ config(
    tags = ['silver', 'emploi'],
    alias = 'emploi_bmo_avec_communes'
) }}

WITH
    bmo AS (
        SELECT
            "annee",
            "Code métier BMO",
            "Nom métier BMO",
            "Famille_met",
            "Lbl_fam_met",
            "REG",
            "NOM_REG",
            LPAD("Dept"::text, 2, '0') AS "Dept",
            "NomDept",
            "BE24",
            "NOMBE24",
            CASE
                WHEN met = '*' THEN 1
                ELSE met::integer
            END AS "met",
            CASE
                WHEN xmet = '*' THEN 1
                ELSE xmet::integer
            END AS "xmet",
            CASE
                WHEN smet = '*' THEN 1
                ELSE smet::integer
            END AS "smet"
        FROM {{ source('bronze', 'vw_emploi_bmo_2024') }}
    ),
    bmo_sum_dept AS (
        SELECT
            "annee",
            "Code métier BMO",
            "Nom métier BMO",
            "Famille_met",
            "Lbl_fam_met",
            "REG",
            "NOM_REG",
            "BE24",
            "NOMBE24",
            SUM(met) AS "met",
            SUM(xmet) AS "xmet",
            SUM(smet) AS "smet"
        FROM bmo
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
    departements AS (
        SELECT DISTINCT
            "Dept",
            "NomDept"
        FROM bmo
    ),
    bassins_communes AS (
        SELECT
            LPAD("dep"::text, 2, '0') AS "dep",
            "code_commune_insee",
            "code_bassin_BMO"
        FROM {{ ref('bassin_emploi') }}
    ),
    bmo_be_sum_communes AS (
        SELECT
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
        FROM bassins_communes
        LEFT JOIN bmo_sum_dept
            ON bassins_communes."code_bassin_BMO" = bmo_sum_dept."BE24"
        LEFT JOIN departements
            ON bassins_communes."dep" = departements."Dept"
    )
SELECT * FROM bmo_be_sum_communes