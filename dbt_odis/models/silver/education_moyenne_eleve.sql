{{ config(
    alias = 'silver_education_moyenne_eleve'
    )
}}



WITH geo AS (
    SELECT
    UPPER(REPLACE(nom, ' ', '-')) AS commune_formate,
    UPPER(REPLACE(departement_nom, ' ', '-')) AS departement_formate,
    UPPER(REPLACE(region_nom, ' ', '-')) AS region_formate
    FROM {{ ref('geographical_references') }} AS A
),

commune AS (
    SELECT 
        B.id,
        REPLACE(commune, ' ', '-') AS commune_formate,
        B.moyenne_eleves_par_classe AS moyenne_eleves_par_classe_commune
    FROM {{ ref('education_moyenne_eleve_commune') }} AS B
),

departement AS (
    SELECT 
    REPLACE(departement, ' ', '-') AS departement_formate,
    moyenne_eleves_par_classe AS moyenne_eleves_par_classe_departement
    FROM {{ ref('education_moyenne_eleve_departement') }} AS C
),

region AS (
    SELECT 
    REPLACE(region_academique, ' ', '-') AS region_formate,
    moyenne_eleves_par_classe AS moyenne_eleves_par_classe_region
    FROM {{ ref('education_moyenne_eleve_region') }} AS D
)


SELECT 
    c.id,
    g.region_formate,
    ROUND(r.moyenne_eleves_par_classe_region::numeric, 2) AS moyenne_eleves_par_classe_region,
    g.departement_formate,
    ROUND(d.moyenne_eleves_par_classe_departement::numeric, 2) AS moyenne_eleves_par_classe_departement,
    g.commune_formate,
    ROUND(c.moyenne_eleves_par_classe_commune::numeric, 2) AS moyenne_eleves_par_classe_commune
FROM commune AS c
LEFT JOIN geo AS g 
    ON g.commune_formate COLLATE "C" = c.commune_formate
LEFT JOIN departement AS d 
    ON g.departement_formate COLLATE "C" = d.departement_formate
LEFT JOIN region AS r 
    ON g.region_formate COLLATE "C" = r.region_formate

;

#probleme dans la table geo il y a des accents c'est pour ça qu'il manque des valeurs







