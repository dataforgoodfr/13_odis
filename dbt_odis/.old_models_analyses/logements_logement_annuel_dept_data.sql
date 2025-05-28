{{ config(
    tags = ['bronze', 'logement'],
    alias = 'vw_logement_annuel_dept_data'
    )
}}

with dept_data as (
    select
        id as id,
        annee as annee,
        departement_code as code_dep,
        departement_libelle as nom_dep,
        type_lgt as type_logement,
        log_aut,
        log_com,
        sdp_aut,
        sdp_com,
        created_at as created_at

      from {{ source('bronze', 'logement_annual_dept_data') }}
  )
  select * from dept_data
    