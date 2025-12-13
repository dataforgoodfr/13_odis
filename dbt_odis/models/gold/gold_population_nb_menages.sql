{{ config(
    tags = ['gold', 'population', 'nb_menages'],
    )
}}

select * from {{ref('silver_population_menages')}}