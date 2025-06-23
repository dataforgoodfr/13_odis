{{ config(
    tags = ['gold', 'emploi'],
    alias = 'gold_emploi_demandeur'
    )
}}

select
    case
        when type_geo = 'commune' then code_geo
        when type_geo = 'departement' then code_geo
        when type_geo = 'region' then concat('reg', code_geo)
        else code_geo
    end as codgeo,
    concat(mois,'_',annee) as "YEAR",
    demandeurs_emploi as "Demandeurs_Emploi"
from {{ ref("emploi_demandeur_pivot")}}