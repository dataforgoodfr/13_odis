{{ config(
    tags = ['bronze', 'geographical_references'],
    alias = 'vw_geographical_references_arrondissements'
    )
}}


with arrondissements as 
(
    select 
        id, 
        (data::jsonb) ->> 'nom' ::text as nom, 
        (data::jsonb) ->> 'code'::text as code_geo, 
        (data::jsonb) ->> 'codeRegion'::text as code_region,
        (data::jsonb) ->> 'codeDepartement'::text as code_departement,
        (data::jsonb) -> 'codesPostaux' ->> 0 as code_postal,
        (data::jsonb) -> 'codesPostaux' ->> 1 as code_postal_2,
        (data::jsonb) -> 'population' as population,        
        created_at
    from {{ source('bronze', 'geographical_references_arrondissements') }} 
),

tab1 as (
    select 
        id, 
        nom, 
        code_geo, 
        code_region, 
        code_departement,
        code_postal,
        population,
        created_at
    from arrondissements
    where code_postal is not null
),

tab2 as (
    select 
        id, 
        nom, 
        code_geo, 
        code_region, 
        code_departement,
        code_postal_2 as code_postal,
        population,
        created_at
    from arrondissements
    where code_postal_2 is not null
)

select * from tab1
union all 
select * from tab2
