{% macro flatten_json(json_column, table_name) %}
    {% set sql %}
        SELECT DISTINCT key
        FROM {{ table_name }},
        LATERAL jsonb_each_text({{ json_column }}) AS key_value
    {% endset %}
    
    {% if execute %}  
        {% set results = run_query(sql) %}
        
        {% if results and results.columns[0].values() is not none %}
            {% set column_names = results.columns[0].values() %}
        {% else %}
            {% set column_names = [] %}
        {% endif %}
    {% else %}
        {% set column_names = ['placeholder_column'] %}
    {% endif %}

    {% for column_name in column_names %}
        data->>'{{ column_name }}' AS "{{ column_name }}"{% if not loop.last %}, {% endif %}
    {% endfor %}
{% endmacro %}
