{% macro cast_column_to_integer(column) %}
    {% if column is not none %}
        cast({{ column }} as integer)
    {% else %}
        null
    {% endif %}
{% endmacro %}