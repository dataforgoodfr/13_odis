{{ config(
    tags = ['silver', 'emploi'],
    alias = 'emploi_bmo_2024_avec_commune'
) }}

with bassin_commune as (
    select
        code_commune_insee,
        "code_bassin_BMO"
    from {{ ref('bassin_emploi') }}
),

bmo as (
    select *
    from {{ source('bronze', 'vw_emploi_bmo_2024') }}
),

jointure as (
    select
        bmo.*,
        bc.code_commune_insee
    from bmo
    left join bassin_commune bc
        on bc."code_bassin_BMO" = bmo."BE24"
)

select
    -- on garde toutes les colonnes sauf 'met' que l'on traite :
    annee,
    "Code métier BMO",
    "Nom métier BMO",
    "Famille_met",
    "Lbl_fam_met",
    "REG",
    "NOM_REG",
    "Dept",
    "NomDept",
    "BE24",
    "NOMBE24",
    case
        when met = '*' then 0
        else met::integer
    end as met,
    xmet,
    smet,
    code_commune_insee
from jointure