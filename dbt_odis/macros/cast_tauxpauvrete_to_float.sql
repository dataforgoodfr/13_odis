{% macro cast_tp_to_float(tp_name_in, tp_name_out) %} 
    case 
        when "{{tp_name_in}}" ~ '^[0-9,]+$' then cast(REPLACE("{{tp_name_in}}", ',', '.') as float) --when no data, s is given from insee
        else null -- french uses comma...
    end as "{{tp_name_out}}"
{% endmacro %}