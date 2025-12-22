-- Test de différence symétrique entre silver_population_menages et com_dep_reg
-- Le test échoue si :
-- 1. Des communes de silver_population_menages n'existent pas dans com_dep_reg
-- 2. Des communes de com_dep_reg n'existent pas dans silver_population_menages
-- Note: Les Collectivités d'Outre-Mer (COM, codes >= 975) sont exclues car
--       elles ne sont pas incluses dans les données de recensement population_menages

-- Communes dans silver_population_menages mais pas dans com_dep_reg
select
    spm.codgeo as code_commune,
    'Commune dans silver_population_menages mais absente de com_dep_reg' as erreur,
    spm.nb_menages,
    spm.year
from {{ ref('silver_population_menages') }} spm
left join {{ ref('com_dep_reg') }} cdr
    on spm.codgeo = cdr."CODGEO"
where cdr."CODGEO" is null
    and SUBSTRING(spm.codgeo, 1, 3) < '975'  -- Exclure les COM

union all

-- Communes dans com_dep_reg mais pas dans silver_population_menages
select
    cdr."CODGEO" as code_commune,
    'Commune dans com_dep_reg mais absente de silver_population_menages' as erreur,
    null as nb_menages,
    null as year
from {{ ref('com_dep_reg') }} cdr
left join {{ ref('silver_population_menages') }} spm
    on cdr."CODGEO" = spm.codgeo
where spm.codgeo is null
    and cdr."CODDEP" < '975'  -- Exclure les COM
