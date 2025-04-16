{% macro check_duplicates(model, column) %} 
    select {{ column }}, count(*) 
    from {{ model }} 
    group by {{ column }} having count(*) > 1 
{% endmacro %}