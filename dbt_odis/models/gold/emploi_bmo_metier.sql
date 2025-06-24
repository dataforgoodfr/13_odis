{{ config(
    tags = ['gold', 'emploi'],
    alias = 'emploi_bmo_metier',
    materialized = 'table'
) }}

{% set metiers_query %}
    select distinct "Nom métier BMO"
    from {{ ref('emploi_bmo_2024_avec_commune') }}
    order by 1
{% endset %}

{% set metiers = dbt_utils.get_column_values(
    table=ref('emploi_bmo_2024_avec_commune'),
    column='"Nom métier BMO"'
) %}

with base as (
    select
        code_commune_insee as codgeo,
        "NOMBE24" as "Bassin_Emploi",
        annee as "YEAR",
        "Nom métier BMO" as metier_bmo,
        met
    from {{ ref('emploi_bmo_2024_avec_commune') }}
),

pivoted as (
    select
        codgeo,
        "Bassin_Emploi",
        "YEAR",

        {% for metier in metiers %}
            max(case when metier_bmo = '{{ metier | replace("'", "''") }}' then met end) as "{{ metier }}"
            {% if not loop.last %},{% endif %}
        {% endfor %}

    from base
    group by codgeo, "Bassin_Emploi", "YEAR"
)

select * from pivoted
