{{ config(
    tags = ['silver', 'emploi'],
    alias = 'silver_emploi_demandeur_emploi'
    )
}}

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
{% endfor %}