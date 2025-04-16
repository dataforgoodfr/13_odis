{% macro replace_null(column, default) %}
    coalesce({{ column }}, {{ default }})
{% endmacro %}