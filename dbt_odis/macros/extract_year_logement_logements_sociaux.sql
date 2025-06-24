{% macro extract_year_logement_logements_sociaux(col_name) %}
  {% set parts = col_name.split('_') %}
  {% set last_part = parts[-1] %}
  
  {% if col_name | length > 4 %}
    {# Case 1: last part is 4-digit year #}
    {% if last_part.isdigit() and last_part | length == 4 %}
      {% set year_int = last_part | int %}
      {% if 1900 <= year_int <= 2099 %}
        {{ return(last_part) }}
      {% endif %}
    {% endif %}
    
    {# Case 2: last 4 chars is year (e.g. nb_ls2023) #}
    {% set maybe_year = col_name[-4:] %}
    {% if maybe_year.isdigit() %}
      {% set year_int = maybe_year | int %}
      {% if 1900 <= year_int <= 2099 %}
        {{ return(maybe_year) }}
      {% endif %}
    {% endif %}
  {% endif %}

  {{ return(None) }}
{% endmacro %}
