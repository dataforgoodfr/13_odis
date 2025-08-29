{{ config(
    alias='silver_population_categorie_socio_pro',
    tags=['silver', 'population_categorie_socio_pro']
) }}



    select * 
    from {{ ref('population_categorie_socio_pro') }}  