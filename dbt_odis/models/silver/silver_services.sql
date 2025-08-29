{{ config(
    tags = ['silver', 'services'],
    alias = 'silver_services'
    )
}}

select 
    geo,
    time_period,
    bpe_measure,
    facility_dom,
    facility_sdom,
    case
        when facility_type = 'A122' then 'Réseau de proximité Pôle emploi'
        when facility_type = 'A128' then 'Implantations France services'
        when facility_type = 'A203' then 'Banques, caisses d''épargne'
        when facility_type = 'A206' then 'Bureau de poste'
        when facility_type = 'A208' then 'Agence postale'
        when facility_type = 'A304' then 'École de conduite'
        when facility_type = 'A501' then 'Salon de Coiffure'
        when facility_type = 'A503' then 'Agence de travail temporaire'
        when facility_type = 'A504' then 'Restaurant ou Restauration rapide'
        else 'Autre'
    end as facility_type,
    cast(value as float) as value,
    case
        when geo like '%COM%' then 'commune'
        when geo like '%DEP%' then 'departement'
        when geo like '%REG%' then 'region'
    end as type_geo,
    case
        when geo like '%COM%' then right(geo, 5)
        when geo like '%DEP%' then replace(right(geo, 3), '-', '')
        when geo like '%REG%' then right(geo, 2)
    end as code_geo
from {{ref ("services_services")}}





 

