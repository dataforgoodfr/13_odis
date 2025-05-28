{{ config(
    tags = ['silver', 'emploi'],
    alias='silver_emploi_eff_secteur_prive_gds_secteurs'
    )
}}
{% set annees = range(2006, 2024) %}
{% for annee in annees %}
select
    concat(id,'-','{{ annee }}') as id,
    region,
    ancienne_region,
    departement,
    zone_d_emploi,
    epci,
    commune,
    intitule_commune,
    grand_secteur_d_activite,
    secteur_na17,
    secteur_na38,
    secteur_na88,
    ape,
    code_region,
    code_ancienne_region,
    code_departement,
    code_zone_d_emploi,
    code_epci,
    code_commune,
    code_ape,
    '{{ annee }}' AS annee,
    cast(nombre_d_etablissements_{{ annee }} as float) as nombre_d_etablissements,
    cast(effectifs_salaries_{{ annee }} as float) as effectifs_salaries
from {{ ref("emploi_eff_prive_gds_secteurs") }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}