{% macro build_historical_indicator_cte(relation, geo_cols, unsuffixed_col_name, suffix_prefix, exclusion_prefixes=[]) %}
  {%- set columns = adapter.get_columns_in_relation(relation) -%}
  {%- set years = [] -%}
  {%- set geo_cols_str = geo_cols | join(', ') %}
  {%- set union_blocks = [] %}

  {# Identify the latest available year from historical columns #}
  {% for col in columns %}
    {% if col.name.startswith(suffix_prefix) and col.name != unsuffixed_col_name %}
    {% set exclude_check = namespace(flag=false) %}
      {% for prefix in exclusion_prefixes %}
        {% if col.name.startswith(prefix) %}
          {% set exclude_check.flag = true %}
        {% endif %}
      {% endfor %}

      {% if not exclude_check.flag %}
        {% set extracted_year = extract_year_logement_logements_sociaux(col.name) %}
        {% if extracted_year is not none %}
          {% do years.append(extracted_year | int) %}
        {% endif %}
      {% endif %}
    {% endif %}
  {% endfor %}

  {% set latest_year = years | max if years | length > 0 else none %}
  {% set next_year = (latest_year + 1) if latest_year else none %}

  {# Include unsuffixed column, assuming it represents the next year #}
  {% for col in columns %}
    {% if col.name == unsuffixed_col_name and next_year is not none %}
      {% set sql = "SELECT " ~ geo_cols_str ~ ", " ~ next_year ~ " AS year, " ~ unsuffixed_col_name ~ " AS indicateur FROM " ~ relation %}
      {% do union_blocks.append(sql) %}
    {% endif %}
  {% endfor %}

  {# Filter historical columns again for final SELECTs #}
  {% for col in columns %}
    {% if col.name.startswith(suffix_prefix) and col.name != unsuffixed_col_name %}
      {% set exclude_check = namespace(flag=false) %}
      {% for prefix in exclusion_prefixes %}
        {% if col.name.startswith(prefix) %}
          {% set exclude_check.flag = true %}
        {% endif %}
      {% endfor %}
      {% if not exclude_check.flag %}
        {% set extracted_year = extract_year_logement_logements_sociaux(col.name) %}
        {% if extracted_year is not none %}
          {% set sql = "SELECT " ~ geo_cols_str ~ ", " ~ extracted_year ~ " AS year, " ~ col.name ~ " AS indicateur FROM " ~ relation %}
          {% do union_blocks.append(sql) %}
        {% endif %}
      {% endif %}
    {% endif %}
  {% endfor %}

  {{ return(union_blocks | join('\nUNION ALL\n')) }}
{% endmacro %}
