{{ config(
    tags = ['bronze', 'emploi'],
    alias = 'vw_emploi_demandeur_emploi_regions'
    )
}}

select 
    id,
    mois as zone_geo,
    {% for column in dbt_utils.get_filtered_columns_in_relation(
        source('bronze', 'emploi_demandeur_emploi_regions'),
        ['id', 'mois', 'created_at']
    ) %}
        {{cast_to_integer(column)}} as {{ column }},
    {% endfor %}
    created_at
from {{ source('bronze', 'emploi_demandeur_emploi_regions') }}