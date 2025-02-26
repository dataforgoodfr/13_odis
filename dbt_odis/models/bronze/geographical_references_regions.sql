{{ config(
    alias = 'vw_geographical_references_regions'
    )
}}


with regions as 
(
    select 
        id as id, 
        data -> 'uri' as uri, 
        data -> 'code' as code, 
        data -> 'type' as type, 
        data -> 'chefLieu' as chef_lieu, 
        data -> 'intitule' as intitule, 
        data -> 'typeArticle' as type_article, 
        data -> 'dateCreation' as date_creation,
        data -> 'intituleSansArticle' as intitule_sans_article,
        created_at as created_at

    from {{ source('bronze', 'geographical_references_regions') }}  
)

select * from regions


