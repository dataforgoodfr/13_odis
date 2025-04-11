{% macro generate_description(model_name, source_table, purpose) %}
    "Ce modèle {{ model_name }}, aggrège les données depuis {{ source_table }}, dans le but de {{ purpose }}."
{% endmacro %}