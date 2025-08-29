{{ config(
    tags = ['gold', 'emploi'],
    alias = 'emploi_bmo_recrutement',
    materialized = 'table'
) }}

WITH base AS (
    SELECT
        "code_commune_insee" AS codgeo,
        "NOMBE24" AS "Bassin_Emploi",
        "annee" AS "YEAR",
        "met" AS "Projets de recrutement",
        "xmet",
        "smet"
    FROM {{ ref('emploi_bmo_avec_communes') }}
)

SELECT
    codgeo,
    "Bassin_Emploi",
    "YEAR",
    "Projets de recrutement",
    ROUND(("xmet" * 100.0 / NULLIF("Projets de recrutement", 0)), 1) AS "Difficultés à recruter",
    ROUND(("smet" * 100.0 / NULLIF("Projets de recrutement", 0)), 1) AS "Emplois saisonniers"
FROM base
ORDER BY "Projets de recrutement" DESC