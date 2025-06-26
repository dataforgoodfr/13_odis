{{ config(
    tags = ['gold', 'emploi'],
    alias = 'emploi_bmo_secteur',
    materialized = 'table'
) }}

{% set secteurs_query %}
    select distinct "Lbl_fam_met"
    from {{ ref('emploi_bmo_avec_communes') }}
    order by 1
{% endset %}

{% set secteurs = dbt_utils.get_column_values(
    table=ref('emploi_bmo_avec_communes'),
    column='"Lbl_fam_met"'
) %}

with
    bmo as (
        select *
        from {{ source('silver', 'emploi_bmo_avec_communes') }}
    ),
    bmo_sans_communes as (
        select
            "Code métier BMO",
            "BE24",
            "NOMBE24" as "Bassin_Emploi",
            "annee" as "YEAR",
            "Lbl_fam_met" as "secteur_bmo",
            max("met") as "met"
        from bmo
        group by
            "Code métier BMO",
            "BE24",
            "Bassin_Emploi",
            "YEAR",
            "secteur_bmo"
    ),
    bmo_secteurs as (
        select
            "BE24",
            "Bassin_Emploi",
            "YEAR",
            "secteur_bmo",
            sum("met") as "met"
        from bmo_sans_communes
        group by
            "BE24",
            "Bassin_Emploi",
            "YEAR",
            "secteur_bmo"
    ),
    bassins_communes as (
        select
            "code_commune_insee" as "codgeo",
            "code_bassin_BMO"
        from {{ ref('bassin_emploi') }}
    ),
    bmo_secteurs_par_communes as (
        select
            bassins_communes."codgeo",
            bmo_secteurs."Bassin_Emploi",
            bmo_secteurs."YEAR",
            bmo_secteurs."secteur_bmo",
            bmo_secteurs."met"
        from bassins_communes
        left join bmo_secteurs
        on bassins_communes."code_bassin_BMO" = bmo_secteurs."BE24"
    ),
pivoted as (
    select
        codgeo,
        "Bassin_Emploi",
        "YEAR",

        {% for secteur in secteurs %}
            max(case when secteur_bmo = '{{ secteur | replace("'", "''") }}' then met end) as "{{ secteur }}"
            {% if not loop.last %},{% endif %}
        {% endfor %} 

    from bmo_secteurs_par_communes
    group by codgeo, "Bassin_Emploi", "YEAR"
)

select * from pivoted