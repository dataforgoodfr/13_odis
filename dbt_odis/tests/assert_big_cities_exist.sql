with big_cities as (
      select '75056' as codgeo, 'Paris' as ville
      union all
      select '13055', 'Marseille'
      union all
      select '69123', 'Lyon'
  ),

  missing as (
      select
          bc.codgeo,
          bc.ville
      from big_cities bc
      left join {{ ref('gold_presentation_population_communes') }} gpc
          on bc.codgeo = gpc.codgeo
      where gpc.codgeo is null
  )

  -- Le test échoue si le code commune de Paris, Marseille ou Lyon manque
  select * from missing