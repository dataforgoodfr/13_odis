{% macro standardize_column_name(column_name) %}
    lower(replace({{ column_name }}, ' ', '_'))
{% endmacro %}