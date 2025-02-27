{{ config(
    alias = 'vw_geographical_references_communes'
    )
}}


with communes as 
(
    select 
        id as id, 
        json_value(data, '$.uri') as uri, 
        json_value(data, '$.code') as code, 
        json_value(data, '$.type') as type,  
        json_value(data, '$.intitule') as intitule, 
        json_value(data, '$.typeArticle') as type_article, 
        json_value(data, '$.dateCreation') as date_creation,
        json_value(data, '$.intituleSansArticle') as intitule_sans_article,
        created_at as created_at
    from {{ source('bronze', 'geographical_references_communes') }} 
)

select * from communes
