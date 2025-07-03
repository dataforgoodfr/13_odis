{{ config(
    tags = ['gold', 'education'],
    alias = 'gold_education_moyenne'
    )
}}

with aggregation as (
  select
    code_geo,
    rentree_scolaire,
    type_geo,
    sum(nombre_total_eleves) as nombre_total_eleves,
    sum(nombre_total_classes) as nombre_total_classes
  from {{ ref('education_moyenne_eleve') }}
  where code_geo is not null
  group by code_geo, rentree_scolaire, type_geo
)


select
  case 
    when type_geo = 'region' then concat('reg',code_geo)
    else code_geo
  end as codgeo,
  extract(year from cast(rentree_scolaire || '-01-01' as date)) as "YEAR",
  case
    when nombre_total_classes != 0 then round(nombre_total_eleves::numeric / nombre_total_classes::numeric,2)
  else null
  end as "Nombre_Moyen_Eleves_Classe",
  nombre_total_eleves as "Nombre total d'élèves",
  nombre_total_classes as "Nombre total de classes"
from aggregation
where rentree_scolaire ~ '^\d{4}$'

