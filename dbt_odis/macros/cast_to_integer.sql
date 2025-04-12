{% macro cast_to_integer(column) %}
    {% if column is not none %}
        cast(replace({{ column }}, ' ', '') as integer)
    {% else %}
        null
    {% endif %}
{% endmacro %}