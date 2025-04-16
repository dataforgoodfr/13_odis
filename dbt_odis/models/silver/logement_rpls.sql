{{ config(alias = 'silver_logement_rpls') }}

{% set table_ref = ref('logement_rpls_region') %}
{% set prefix_1 = 'tx_vac_' %}
{% set prefix_2 = 'tx_vac_3_' %}

{% set all_columns = get_columns_in_relation(table_ref) %}

{% set filtered_columns = [] %}

{# Filtrer les colonnes qui commencent par les préfixes #}
{% for col in all_columns %}
    {% if col.startswith(prefix_1) and col | replace(prefix_1, '') | regex_search('^[0-9]{4}$') %}
        {% do filtered_columns.append(col) %}
    {% elif col.startswith(prefix_2) and col | replace(prefix_2, '') | regex_search('^[0-9]{4}$') %}
        {% do filtered_columns.append(col) %}
    {% endif %}
{% endfor %}

{% if filtered_columns | length == 0 %}

    select NULL as densite, NULL as tx_vac, NULL as annee, NULL as type

{% else %}

    with logement_rpls as (

        {% for col in filtered_columns %}
            {% if not loop.first %} union all {% endif %}

            select
                densite,
                {{ col }} as tx_vac,
                '{{ col | regex_search('[0-9]{4}$') }}' as annee,
                '{% if col.startswith(prefix_2) %}tx_vac_3{% else %}tx_vac{% endif %}' as type
            from {{ table_ref }}
        {% endfor %}

    )

    select * from logement_rpls

{% endif %}