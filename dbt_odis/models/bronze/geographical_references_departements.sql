{{ config(
    tags = ['bronze', 'geographical_references'],
    alias = 'vw_geographical_references_departements'
    )
}}


with departments as 
(
    select 
        id, 
        (data::jsonb) ->> 'uri' as uri, 
        (data::jsonb) ->> 'code' as code, 
        (data::jsonb) ->> 'type' as type,  
        (data::jsonb) ->> 'chefLieu' as chef_lieu,
        (data::jsonb) ->> 'intitule' as intitule, 
        (data::jsonb) ->> 'typeArticle' as type_article, 
        (data::jsonb) ->> 'dateCreation' as date_creation,
        (data::jsonb) ->> 'intituleSansArticle' as intitule_sans_article,
        created_at

    from {{ source('bronze', 'geographical_references_departements') }}  
)

select * from departments
