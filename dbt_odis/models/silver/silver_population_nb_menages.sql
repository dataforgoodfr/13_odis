{{ config(
    alias='silver_population_nb_menages',
    tags=['silver', 'population_nb_menages']
) }}



    select * 
    from {{ ref('population_nb_menages') }}  