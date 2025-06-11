{{ config(
    tags = ['gold', 'geographical_references'],
    alias = 'gold_education_moyenne'
    )
}}

{# #select les colonnes du silver pour correspondre à la table du drive  #}



Select *
    from {{ ref('education_moyenne_eleve') }}