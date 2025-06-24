{{ config(
    alias='silver_population_categorie_socio_pro',
    tags=['silver', 'population_categorie_socio_pro']
) }}


with population_csp as(
    select * 
    from {{ ref('population_categorie_socio_pro') }}  
)    