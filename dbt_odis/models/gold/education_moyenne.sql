{{ config(
    tags = ['gold', 'education'],
    alias = 'gold_education_moyenne'
    )
}}

{# #select les colonnes du silver pour correspondre à la table du drive  #}



Select *
    from {{ ref('education_moyenne_eleve') }}