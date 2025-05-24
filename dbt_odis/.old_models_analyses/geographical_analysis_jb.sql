--geographical_references_regions
select
    id as id,
    JSON_VALUE (data, '$.uri') as uri,
    JSON_VALUE (data, '$.code') as code,
    JSON_VALUE (data, '$.type') as type,
    JSON_VALUE (data, '$.chefLieu') as chef_lieu,
    JSON_VALUE (data, '$.intitule') as intitule,
    JSON_VALUE (data, '$.typeArticle') as type_article,
    JSON_VALUE (data, '$.dateCreation') as date_creation,
    JSON_VALUE (data, '$.intituleSansArticle') as intitule_sans_article,
    created_at as created_at
from
    odis.bronze.geographical_references_regions;

-- geographical_references_departments
select
    id as id,
    JSON_VALUE (data, '$.uri') as uri,
    JSON_VALUE (data, '$.code') as code,
    JSON_VALUE (data, '$.type') as type,
    JSON_VALUE (data, '$.chefLieu') as chef_lieu,
    JSON_VALUE (data, '$.intitule') as intitule,
    JSON_VALUE (data, '$.typeArticle') as type_article,
    JSON_VALUE (data, '$.dateCreation') as date_creation,
    JSON_VALUE (data, '$.intituleSansArticle') as intitule_sans_article,
    created_at as created_at
from
    odis.bronze.geographical_references_departments;

-- geographical_references_communes
select
    id as id,
    JSON_VALUE (data, '$.uri') as uri,
    JSON_VALUE (data, '$.code') as code,
    JSON_VALUE (data, '$.type') as type,
    JSON_VALUE (data, '$.intitule') as intitule,
    JSON_VALUE (data, '$.typeArticle') as type_article,
    JSON_VALUE (data, '$.dateCreation') as date_creation,
    JSON_VALUE (data, '$.intituleSansArticle') as intitule_sans_article,
    created_at as created_at
from
    odis.bronze.geographical_references_communes;
