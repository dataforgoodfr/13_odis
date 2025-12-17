{{ config(
    tags = ['bronze', 'population'],
    alias='vw_population_nb_menages'
    )
}}

{% set local_year = '2021' %}

with population_nb_menages as 
(
    select {{ dbt_utils.star(from=source('bronze', 'population_menages_td_men1_2021')) }},
    '{{ local_year }}' as "year"
    from {{ source('bronze', 'population_menages_td_men1_2021') }}
)

select * from population_nb_menages