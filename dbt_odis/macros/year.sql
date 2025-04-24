{%- macro year(date) -%}
    {{ adapter.dispatch('year', 'dbt_date') (date) }}
{%- endmacro %}

{%- macro default__year(date) -%}
    extract(year from {{ date }})
{%- endmacro %}

{%- macro postgres__year(date) -%}
    extract(year from {{ date }})
{%- endmacro %}

{%- macro redshift__year(date) -%}
    extract(year from {{ date }})
{%- endmacro %}

{%- macro spark__year(date) -%}
    year({{ date }})
{%- endmacro %}

{%- macro trino__year(date) -%}
    year({{ date }})
{%- endmacro %}