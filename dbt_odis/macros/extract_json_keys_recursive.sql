{% macro extract_json_keys_recursive(table_ref, json_column, sample_condition="true", max_depth=5, max_fields=500) %}

{% set query %}
with recursive paths as (
    -- 1. LIGNE D’ENTRÉE UNIQUE
    select
        key::text as root,
        '' as path,
        value,
        jsonb_typeof(value) as type,
        1 as level
    from (
        select * from {{ table_ref }} where {{ sample_condition }} limit 1
    ) as sample,
         jsonb_each((sample.{{ json_column }}::jsonb))
    where key in ('dimensions', 'measures')

    union all

    -- 2. RÉCURRENCE LIMITÉE PAR PROFONDEUR
    select
        p.root,
        (p.path || '.' || je.key)::text as path,
        je.val,
        jsonb_typeof(je.val),
        p.level + 1
    from paths p
    join lateral jsonb_each(p.value) as je(key, val)
        on p.type = 'object'
    where p.level < {{ max_depth }}
)

select root, path, type
from paths
where type in ('text','string', 'number', 'boolean')
limit {{ max_fields }}
{% endset %}

{# Exécution SQL #}
{% set res = run_query(query) %}

{% if execute and res %}
    {% set roots = res.columns[0].values() %}
    {% set paths = res.columns[1].values() %}
    {% set types = res.columns[2].values() %}
{% else %}
    {% set roots = [] %}
    {% set paths = [] %}
    {% set types = [] %}
{% endif %}

{# Construction du SELECT final #}
{% if paths | length == 0 %}
    null as empty_data
{% else %}
    {% for root, path, ptype in zip(roots, paths, types) %}
        {%- set keys = [root] + path.split('.') if path else [root] %}
        {%- set expr = "(" ~ json_column ~ " :: jsonb)" %}
        {%- for key in keys[:-1] %}
            {%- set expr = expr ~ "->'" ~ key ~ "'" %}
        {%- endfor %}
        {%- set expr = expr ~ "->>'" ~ keys[-1] ~ "'" %}

        {%- if 'value' in path or ptype == 'string' %}
            {%- set cast = '::text' %}
        {%- elif ptype == 'number' %}
            {%- set cast = '::numeric' %}
        {%- elif ptype == 'boolean' %}
            {%- set cast = '::boolean' %}
        {%- else %}
            {%- set cast = '::text' %}
        {%- endif %}

        {%- set alias = keys | join('_') | replace('.', '_') %}
        {{ expr ~ cast }} as {{ alias }}{% if not loop.last %}, {% endif %}
    {% endfor %}
{% endif %}

{% endmacro %}
