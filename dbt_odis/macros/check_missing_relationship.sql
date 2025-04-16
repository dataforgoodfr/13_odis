{% macro check_missing_relationship(model, foreign_key, reference_model, reference_column) %}
    select * from {{ model }} 
    where {{ foreign_key }} not in (select {{ reference_column }} from {{ reference_model }})
{% endmacro %}