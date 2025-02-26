--geographical_references_regions

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
from odis.bronze.geographical_references_regions 
limit 5
; 

-- geographical_references_departments
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
from odis.bronze.geographical_references_departments 
limit 5
; 

-- geographical_references_communes
select 
    id as id, 
    data -> 'uri' as uri, 
    data -> 'code' as code, 
    data -> 'type' as type,  
    data -> 'intitule' as intitule, 
    data -> 'typeArticle' as type_article, 
    data -> 'dateCreation' as date_creation,
    data -> 'intituleSansArticle' as intitule_sans_article,
    created_at as created_at
from odis.bronze.geographical_references_departments 
limit 5
; 
