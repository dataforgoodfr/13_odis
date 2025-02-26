{{ config(
    alias = 'vw_geographical_references_communes'
    )
}}


with communes as 
(
    select 
        id as id, 
        JSON_VALUE(data, '$.uri') as uri, 
        JSON_VALUE(data, '$.code') as code, 
        JSON_VALUE(data, '$.type') as type,  
        JSON_VALUE(data, '$.intitule') as intitule, 
        JSON_VALUE(data, '$.typeArticle') as type_article, 
        JSON_VALUE(data, '$.dateCreation') as date_creation,
        JSON_VALUE(data, '$.intituleSansArticle') as intitule_sans_article,
        created_at as created_at
    from odis.bronze.geographical_references_communes

select * from communes
