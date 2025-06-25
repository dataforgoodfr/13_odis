{% macro get_latest_year(source_table, indicator_prefix) %}
    {% set cols = adapter.get_columns_in_relation(source('bronze', source_table)) %}
    {% set years = [] %}

    {% for col in cols %}
        {% if col.name.startswith(indicator_prefix) %}
            {% set year_str = col.name.replace(indicator_prefix, '') %}
            {% if year_str.isdigit() %}
                {% do years.append(year_str | int) %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {% if years | length > 0 %}
        {% set latest = years | max %}
        {% do return(latest) %}
    {% else %}
        {% do return(none) %}
    {% endif %}
{% endmacro %}



