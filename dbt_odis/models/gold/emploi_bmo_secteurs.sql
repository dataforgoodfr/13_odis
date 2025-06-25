{{ config(
    tags = ['gold', 'emploi'],
    alias = 'emploi_bmo_secteur',
    materialized = 'table'
) }}

{% set secteurs_query %}
    select distinct "intitule_fap_22"
    from {{ ref('emploi_bmo_2024_avec_commune') }}
    order by 1
{% endset %}

{% set secteurs = dbt_utils.get_column_values(
    table=ref('emploi_bmo_2024_avec_commune'),
    column='"intitule_fap_22"'
) %}

with base as (
    select
        code_commune_insee as codgeo,
        "NOMBE24" as "Bassin_Emploi",
        annee as "YEAR",
        "intitule_fap_22" as secteur_bmo,
        met
    from {{ ref('emploi_bmo_2024_avec_commune') }}
),

pivoted as (
    select
        codgeo,
        "Bassin_Emploi",
        "YEAR",

        {% for secteur in secteurs %}
            sum(case when secteur_bmo = '{{ secteur | replace("'", "''") }}' then met end) as "{{ secteur }}"
            {% if not loop.last %},{% endif %}
        {% endfor %}

    from base
    group by codgeo, "Bassin_Emploi", "YEAR"
)

select * from pivoted
