{% macro generate_flatten_json_logement(model_name, json_column) %}

{% set get_json_path %}
    with json_flatten as (
        select 
            f.path as json_path,
            typeof(f.value) as json_datatype
        from {{ model_name }},
        lateral flatten(input => {{ json_column }}, recursive => true) f
    )
    select distinct json_path, json_datatype
    from json_flatten
{% endset %}

{% set res = run_query(get_json_path) %}

{% if execute and res %}
    {% set res_list = res.columns[0].values() %}
    {% set res_list_path = res.columns[1].values() %}
{% else %}
    {% set res_list = [] %}
    {% set res_list_path = [] %}
{% endif %}

{% for json_path_type, json_path in zip(res_list, res_list_path) %}
    {{ json_column }}:{{ json_path_type }} as {{ json_path }}{% if not loop.last %},{% endif %}
{% endfor %}

{% endmacro %}