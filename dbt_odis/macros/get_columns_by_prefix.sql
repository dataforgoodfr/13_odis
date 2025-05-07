{% macro get_columns_by_prefix(table_ref, column_prefix) %}
 {% set columns = [] %}
 {% set schema_name = table_ref.schema %}
 {% set table_name = table_ref.identifier %}
 {% set query %}
   SELECT column_name
   FROM information_schema.columns
   WHERE table_schema = '{{ schema_name }}'
   AND table_name = '{{ table_name }}'
   AND column_name LIKE '{{ column_prefix }}'
 {% endset %}
 {% set results = run_query(query) %}
  {% for row in results %}
   {% do columns.append(row['column_name']) %}
 {% endfor %}
 {{ return(columns) }}
{% endmacro %}