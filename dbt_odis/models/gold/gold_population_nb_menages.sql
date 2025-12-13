{{ config(
    tags = ['gold', 'population', 'nb_menages'],
    )
}}

select * from {{ref('silver_population_menages')}}
union all
select * from {{ref('silver_population_menages_departement')}}
union all
select * from {{ref('silver_population_menages_region')}}