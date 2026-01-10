{% test assert_big_cities_exist(model, column_name) %}

with big_cities as (
      select '75056' as codgeo
      union all
      select '13055'
      union all
      select '69123'
  ),

  missing as (
      select
          bc.codgeo
      from big_cities bc
      left join {{ model }} model
          on bc.codgeo = model.{{column_name}}
      where model.{{column_name}} is null
  )

  -- Le test échoue si le code commune de Paris, Marseille ou Lyon manque
  select * from missing

{% endtest %}