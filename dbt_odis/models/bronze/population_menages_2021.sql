{{ config(
    tags = ['bronze', 'population'],
    alias='vw_population_menages_2021'
    )
}}

select 
    {{ dbt_utils.star(from=source('bronze', 'population_menages_td_men1_2021')) }}
from {{ source('bronze', 'population_menages_td_men1_2021') }}