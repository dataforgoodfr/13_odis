{{ config(
    alias = 'geographical_references_communes'
    )
}}


with communes as 
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

    from {{ source('bronze', 'geographical_references_communes') }}  
)

select * from communes
