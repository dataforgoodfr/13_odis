{% macro infer_current_year_from_historical_columns(relation) %}
    {% set columns = adapter.get_columns_in_relation(relation) %}

    {% set years = [] %}

    {% for col in columns %}
        {% set extracted_year = extract_year_logement_logements_sociaux(col.name) %}
        {% if extracted_year is not none %}
            {% do years.append(extracted_year | int) %}
        {% endif %}
    {% endfor %}

    {% if years | length > 0 %}
        {{ return((years | max) + 1) }}
    {% else %}
        {{ return(None) }}
    {% endif %}
{% endmacro %}
