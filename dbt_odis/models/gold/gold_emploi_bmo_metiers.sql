{{ config(
    tags = ['gold', 'emploi'],
    alias = 'gold_emploi_bmo_metiers'
) }}

with agg_metiers as (

    select 
        code_geo as codgeo,
        "NOMBE24" as "Bassin_Emploi",
        "YEAR",
        "Nom métier BMO",
        sum(met) as met
        from {{ ref('silver_emploi_bmo') }}
        group by 
            codgeo,
            "NOMBE24",
            "YEAR",
            "Nom métier BMO"
)

{% set secteurs_list = dbt_utils.get_column_values(
    ref('silver_emploi_bmo'),
    '"Nom métier BMO"'
) %}
    
select
    codgeo,
    "Bassin_Emploi",
    "YEAR",
{% for secteur in secteurs_list %}
        max(case when "Nom métier BMO" = '{{ secteur | replace("'", "''") }}' then met end) as "{{ secteur }}"
        {% if not loop.last %},{% endif %}
        {% endfor %}
from agg_metiers
group by
    codgeo,
    "Bassin_Emploi",
    "YEAR"