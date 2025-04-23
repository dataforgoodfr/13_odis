{% macro generate_flatten_json_education(model_name, json_column) %}

{% set query %}
    with json_flatten as (
        select 
            f.path as json_path,
            case 
                when typeof(f.value) = 'VARCHAR' then 'STRING'
                when typeof(f.value) = 'INTEGER' then 'NUMBER'
                when typeof(f.value) = 'FLOAT' then 'NUMBER'
                else typeof(f.value)
            end as json_datatype
        from {{ model_name }},
        lateral flatten(input => {{ json_column }}, recursive => true) f
        where typeof(f.value) in ('VARCHAR', 'INTEGER', 'FLOAT')
    )
    select distinct json_path, json_datatype
    from json_flatten
    where json_datatype not in ('NULL_VALUE', 'OBJECT')
{% endset %}

{% set res = run_query(query) %}

{% if execute and res %}
    {% set paths = res.columns[0].values() %}
    {% set types = res.columns[1].values() %}
{% else %}
    {% set paths = [] %}
    {% set types = [] %}
{% endif %}

{% if paths | length == 0 %}
    null as dummy_column
{% else %}
    {% for path, dtype in zip(paths, types) %}
        {{ json_column }}:{{ path }}::{{ dtype }} as {{ path | replace('.', '_') | replace('[', '_') | replace(']', '') }}{% if not loop.last %},{% endif %}
    {% endfor %}
{% endif %}

{% endmacro %}
