{% macro generate_flatten_json(model_name, json_column) %}

{% set get_json_path %}

with json_flatten as (
select 
  j.path as json_path,
  case when typeof(f.value)='VARCHAR' then 'STRING' 
    else 'NUMBER' end 
  as json_datatype
from {{ model_name }},
lateral flatten(input => {{ json_column }}, recursive => true ) j
where typeof(f.value) in ('VARCHAR','INTEGER')
)

 select distinct concat(json_path,'::',json_datatype),json_path
 from json_flatten
 where json_datatype not in ('NULL_VALUE','OBJECT')

{% endset %}

{% set res = run_query(get_json_path) %}
{% if execute %}
    {% set res_list = res.columns[0].values() %}
    {% set res_list_path = res.columns[1].values() %}
{% else %}
    {% set res_list = [] %}
{% endif %}

select 
{% for json_path_type,json_path in zip(res_list,res_list_path) %}
    {{ json_column }}:{{ json_path_type }} as {{ json_path }}
{% endfor %}

from {{ model_name }}

{% endmacro %}