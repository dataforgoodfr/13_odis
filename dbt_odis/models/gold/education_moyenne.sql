{{ config(
    tags = ['gold', 'education'],
    alias = 'gold_education_moyenne'
    )
}}

{# #select les colonnes du silver pour correspondre à la table du drive  #}


Select
*
from {{ ref('education_moyenne_eleve') }}
WHERE code_geo = 73
 


 


SELECT
  code_geo AS codgeo,
  rentree_scolaire AS year,
  CASE
    WHEN nombre_total_classes::numeric != 0 THEN ROUND(nombre_total_eleves::numeric / nombre_total_classes::numeric,2)
    ELSE NULL
  END AS nombre_moyen_eleves_classe,
  nombre_total_eleves AS "Nombre total d'élèves",
  nombre_total_classes AS "Nombre total de classes"
FROM {{ ref('education_moyenne_eleve') }}
ORDER BY code_geo, rentree_scolaire ASC;


