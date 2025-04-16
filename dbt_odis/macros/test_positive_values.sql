{% macro test_positive_value(model, column) %}
    select * from {{ model }} where {{ column }} < 0
{% endmacro %}