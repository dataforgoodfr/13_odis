--geographical_references_regions
select
    id as id,
    data - > 'uri' as uri,
    data - > 'code' as code,
    data - > 'type' as type,
    data - > 'chefLieu' as chef_lieu,
    data - > 'intitule' as intitule,
    data - > 'typeArticle' as type_article,
    data - > 'dateCreation' as date_creation,
    data - > 'intituleSansArticle' as intitule_sans_article,
    created_at as created_at
from
    odis.bronze.geographical_references_regions
limit
    5;

-- geographical_references_departments
select
    id as id,
    data - > 'uri' as uri,
    data - > 'code' as code,
    data - > 'type' as type,
    data - > 'chefLieu' as chef_lieu,
    data - > 'intitule' as intitule,
    data - > 'typeArticle' as type_article,
    data - > 'dateCreation' as date_creation,
    data - > 'intituleSansArticle' as intitule_sans_article,
    created_at as created_at
from
    odis.bronze.geographical_references_departments
limit
    5;

-- geographical_references_communes
select
    id as id,
    data - > 'uri' as uri,
    data - > 'code' as code,
    data - > 'type' as type,
    data - > 'intitule' as intitule,
    data - > 'typeArticle' as type_article,
    data - > 'dateCreation' as date_creation,
    data - > 'intituleSansArticle' as intitule_sans_article,
    created_at as created_at
from
    odis.bronze.geographical_references_communes
limit
    5;

select
    communes.code as codgeo,
    regions.code as codreg,
    departements.code as coddep,
    communes.intitule as libgeo
from
    odis.bronze.vw_geographical_references_regions regions
    join odis.bronze.vw_geographical_references_departments departments on regions.code = substring(departements.code, 1, 2)
    join odis.bronze.vw_geographical_references_communes communes on departements.code = substring(communes.code, 1, 2)
order by
    communes.code asc;
