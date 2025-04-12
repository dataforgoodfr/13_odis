{{ config(alias = 'logement_rpls') }}


-- depends_on: {{ ref('logement_rpls_region') }}


{% set table_ref = ref('logement_rpls_region') %}
{% set column_prefix = 'tx_vac%' %}
{% set columns = get_columns_by_prefix(table_ref, column_prefix) %}


{% if columns | length == 0 %}
   select NULL as densite, NULL as tx_vac, NULL as annee
{% else %}
   with logement_rpls as (
       {% for col in columns %}
           {% if not loop.first %}
               UNION ALL
           {% endif %}
           select
               densite,
               "{{ col }}" as tx_vac,  -- 🚀 Fix: Quotes column name
               '{{ col.split('_')[2] }}' as annee
           from {{ table_ref }}
       {% endfor %}
   )
   select * from logement_rpls
{% endif %}
