{{ config(
    tags = ['silver', 'emploi'],
    alias='silver_emploi_eff_secteur_prive_gds_secteurs'
    )
}}

{% set colonnes = adapter.get_columns_in_relation(ref('emploi_eff_prive_gds_secteurs')) %}

{% set annees = [] %}
{% for col in colonnes %}
    {% if col.name.startswith('effectifs_salaries_') or col.name.startswith('nombre_d_etablissements_') %}
        {% set annee = col.name.split('_')[-1] %}
        {% if annee.isdigit() and annee|length == 4 and annee not in annees %}
            {% do annees.append(annee) %}
        {% endif %}
    {% endif %}
{% endfor %}

{% set selects = [] %}
{% for annee in annees %}
    {% set select_sql %}
        select
            concat(code_ape,'-',code_commune,'-','{{ annee }}') as id,
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
    {% endset %}
    {% do selects.append(select_sql) %}
{% endfor %}

{{ selects | join(' union all\n') }}