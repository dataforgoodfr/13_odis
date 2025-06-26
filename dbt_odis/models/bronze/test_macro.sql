with geo_commune as (
select
    "Code_commune_INSEE" as code_geo,
    "Nom_de_la_commune" as nom,
    "Code_postal" as code_postal,
    regexp_replace("Intitule",'[-'']', ' ', 'g') as commune
from {{ ref('corresp_codes_communes') }}
group by 
    code_geo,
    nom,
    code_postal,
    commune
),
    
education_communes as (
    select 
        id,
        code_postal,
        commune,
        departement,
        academie,
        region_academique,
        rentree_scolaire,
        nombre_total_eleves,
        nombre_total_classes,
        nombre_eleves_ulis,
        nombre_eleves_cp_hors_ulis,
        nombre_eleves_ce1_hors_ulis,
        nombre_eleves_ce2_hors_ulis,
        nombre_eleves_cm1_hors_ulis,
        nombre_eleves_cm2_hors_ulis,
        nombre_eleves_elementaire_hors_ulis,
        nombre_eleves_preelementaire_hors_ulis,
        created_at,
        regexp_replace(commune,'[-'']', ' ', 'g') as nom_commune
    from {{ ref('education_moyenne_eleve_commune') }}
)
    select 
        gc.code_geo,
        c.id,
        c.code_postal,
        c.commune as nom_geo,
        'commune' as type_geo,
        c.departement,
        c.academie,
        c.region_academique,
        c.rentree_scolaire,
        c.nombre_total_eleves,
        c.nombre_total_classes,
        c.nombre_eleves_ulis,
        c.nombre_eleves_cp_hors_ulis,
        c.nombre_eleves_ce1_hors_ulis,
        c.nombre_eleves_ce2_hors_ulis,
        c.nombre_eleves_cm1_hors_ulis,
        c.nombre_eleves_cm2_hors_ulis,
        c.nombre_eleves_elementaire_hors_ulis,
        c.nombre_eleves_preelementaire_hors_ulis,
        c.created_at
    from education_communes c
        left join geo_commune gc
        on c.code_postal = gc.code_postal
        and '%' || c.nom_commune || '%' like '%' || gc.commune || '%'

{# 
{% set annees = range(2020, 2025) %}
{% set mois = ['janvier', 'fevrier', 'mars', 'avril', 'mai', 'juin', 'juillet', 'aout', 'septembre', 'octobre', 'novembre', 'decembre'] %}

{% set combinaisons = [] %}
{% for annee in annees %}
    {% for m in mois %}
        {% do combinaisons.append({'mois': m, 'annee': annee}) %}
    {% endfor %}
{% endfor %}

{% for combo in combinaisons %}
    select
        concat(code_geo, '-', '{{ combo.mois }}', '-', '{{ combo.annee }}') as id,
        code_geo,
        nom,
        intitule,
        type_geo,
        code_postal,
        zone_geo,
        '{{ combo.mois }}' AS mois,
        '{{ combo.annee }}' AS annee,
        {{ combo.mois }}_{{ combo.annee }} as demandeurs_emploi
    from {{ ref("emploi_demandeur_emploi") }}
    {% if not loop.last %}
    union all
    {% endif %}
{% endfor %} #}