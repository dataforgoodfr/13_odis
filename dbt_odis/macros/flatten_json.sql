{% macro flatten_json(json_column) %}
    {% set sql %}
        SELECT DISTINCT key
        FROM {{ ref('bronze.services_services') }},
        LATERAL jsonb_each_text({{ json_column }}) AS key_value
    {% endset %}

    {% set keys = run_query(sql).columns %}
    {% set column_names = keys | map(attribute='name') %}

    {% for column_name in column_names %}
        data->>{{ column_name }} AS {{ column_name }}
    {% endfor %}
{% endmacro %}