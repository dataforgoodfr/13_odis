{{ config(
    tags = ['gold', 'emploi'],
    alias = 'gold_emploi_bmo_secteurs'
) }}

with agg_secteurs as (

    select 
        code_geo as codgeo,
        "NOMBE24" as "Bassin_Emploi",
        "YEAR",
        "Intitulé_FAP22",
        sum(met) as met
        from {{ ref('silver_emploi_bmo') }}
        group by 
            codgeo,
            "NOMBE24",
            "YEAR",
            "Intitulé_FAP22"
)

{% set secteurs_list = dbt_utils.get_column_values(
    ref('silver_emploi_bmo'),
    '"Intitulé_FAP22"'
) %}
    
select
    codgeo,
    "Bassin_Emploi",
    "YEAR",
{% for secteur in secteurs_list %}
        max(case when "Intitulé_FAP22" = '{{ secteur | replace("'", "''") }}' then met end) as "{{ secteur }}"
        {% if not loop.last %},{% endif %}
        {% endfor %} 
from agg_secteurs
group by
    codgeo,
    "Bassin_Emploi",
    "YEAR"
