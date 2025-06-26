{{ config(
    tags = ['silver', 'emploi'],
    alias = 'silver_emploi_demandeur_emploi'
    )
}}

{% set colonnes = adapter.get_columns_in_relation(ref('emploi_demandeur_emploi')) %}

{% set mois_liste = ['janvier', 'fevrier', 'mars', 'avril', 'mai', 'juin', 'juillet', 'aout', 'septembre', 'octobre', 'novembre', 'decembre'] %}
{% set colonnes_mois_annee = [] %}
{% for col in colonnes %}
    {% for m in mois_liste %}
        {% if col.name.startswith(m + '_') and col.name|length == (m|length + 1 + 4) %}
            {% set annee = col.name.split('_')[1] %}
            {% if annee.isdigit() and annee|length == 4 %}
                {% do colonnes_mois_annee.append(col.name) %}
            {% endif %}
        {% endif %}
    {% endfor %}
{% endfor %}

{% set mois = [] %}
{% set annees = [] %}
{% for col in colonnes_mois_annee %}
    {% set parts = col.split('_') %}
    {% if parts[0] not in mois %}
        {% do mois.append(parts[0]) %}
    {% endif %}
    {% if parts[1] not in annees %}
        {% do annees.append(parts[1]) %}
    {% endif %}
{% endfor %}

{% set selects = [] %}
{% for col in colonnes_mois_annee %}
    {% set parts = col.split('_') %}
    {% set m = parts[0] %}
    {% set annee = parts[1] %}
    {% set select_sql %}
        select
            concat(code_geo, '-', '{{ m }}', '-', '{{ annee }}') as id,
            code_geo,
            nom,
            intitule,
            type_geo,
            code_postal,
            zone_geo,
            '{{ m }}' AS mois,
            '{{ annee }}' AS annee,
            {{ col }} as demandeurs_emploi
        from {{ ref("emploi_demandeur_emploi") }}
    {% endset %}
    {% do selects.append(select_sql) %}
{% endfor %}

{{ selects | join(' union all\n') }}
