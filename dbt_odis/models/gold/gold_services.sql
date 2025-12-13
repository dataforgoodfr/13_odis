{{ config(
    tags = ['gold', 'services'],
    alias = 'gold_services'
    )
}}

with max_year as (
    select max(extract(year from cast(time_period || '-01-01' as date))) as "YEAR"
    from {{ ref('silver_services') }}
),

communes_all as (
    select
        gr.code as codgeo,
        my."YEAR",
        max(case when s.facility_type = 'Réseau de proximité Pôle emploi' then coalesce(s.value, 0) else 0 end) as "Réseau de proximité Pôle emploi",
        max(case when s.facility_type = 'Implantations France services' then coalesce(s.value, 0) else 0 end) as "Implantations France services",
        max(case when s.facility_type = 'Banques, caisses d''épargne' then coalesce(s.value, 0) else 0 end) as "Banques, caisses d'épargne",
        max(case when s.facility_type = 'Bureau de poste' then coalesce(s.value, 0) else 0 end) as "Bureau de poste",
        max(case when s.facility_type = 'Agence postale' then coalesce(s.value, 0) else 0 end) as "Agence postale",
        max(case when s.facility_type = 'École de conduite' then coalesce(s.value, 0) else 0 end) as "École de conduite",
        max(case when s.facility_type = 'Salon de Coiffure' then coalesce(s.value, 0) else 0 end) as "Salon de Coiffure",
        max(case when s.facility_type = 'Agence de travail temporaire' then coalesce(s.value, 0) else 0 end) as "Agence de travail temporaire",
        max(case when s.facility_type = 'Restaurant ou Restauration rapide' then coalesce(s.value, 0) else 0 end) as "Restaurant ou Restauration rapide"
    from {{ ref('geographical_references') }} gr
    cross join max_year my
    left join {{ ref('silver_services') }} s
        on gr.code = s.code_geo
        and extract(year from cast(s.time_period || '-01-01' as date)) = my."YEAR"
    group by gr.code, my."YEAR"
),

dept_reg as (
    select
        case
            when type_geo = 'commune' then code_geo
            when type_geo = 'departement' then code_geo
            when type_geo = 'region' then concat('reg', code_geo)
            else code_geo
        end as codgeo,
        extract(year from cast(time_period || '-01-01' as date)) as "YEAR",
        max(case when facility_type = 'Réseau de proximité Pôle emploi' then value else 0 end) as "Réseau de proximité Pôle emploi",
        max(case when facility_type = 'Implantations France services' then value else 0 end) as "Implantations France services",
        max(case when facility_type = 'Banques, caisses d''épargne' then value else 0 end) as "Banques, caisses d'épargne",
        max(case when facility_type = 'Bureau de poste' then value else 0 end) as "Bureau de poste",
        max(case when facility_type = 'Agence postale' then value else 0 end) as "Agence postale",
        max(case when facility_type = 'École de conduite' then value else 0 end) as "École de conduite",
        max(case when facility_type = 'Salon de Coiffure' then value else 0 end) as "Salon de Coiffure",
        max(case when facility_type = 'Agence de travail temporaire' then value else 0 end) as "Agence de travail temporaire",
        max(case when facility_type = 'Restaurant ou Restauration rapide' then value else 0 end) as "Restaurant ou Restauration rapide"
    from {{ ref('silver_services') }}
    where type_geo in ('departement', 'region')
    group by codgeo, "YEAR"
)

select
    *
from communes_all
union all
select
    *
from dept_reg
