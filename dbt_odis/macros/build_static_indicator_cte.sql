{% macro build_static_indicator_cte(relation, geo_cols, static_cols) %}
  {% if execute %}
    {%- set columns = adapter.get_columns_in_relation(relation) -%}
    {%- set geo_cols_str = geo_cols | join(', ') %}

    {# Use helper macro to infer the most recent year from historical columns #}
    {% set current_year = infer_current_year_from_historical_columns(relation) %}
    {%- if current_year is none %}
        {{ exceptions.raise_compiler_error("Cannot determine next_year for static indicators.") }}
    {%- endif %}

    {# Build the select with all static_cols cast as numeric #}
    {% set select_cols = [] %}
    {% for col in static_cols %}
      {% do select_cols.append(col ~ '::NUMERIC AS ' ~ col) %}
    {% endfor %}

    {% set sql %}
      SELECT
        {{ geo_cols_str }},
        {{ current_year }} AS year,
        {{ select_cols | join(', ') }}
      FROM {{ relation }}
    {% endset %}

    {{ return(sql) }}
  {% else %}
    {{ return("select 1 as dummy") }}
  {% endif %}
{% endmacro %}
