{{ config(alias = 'logement_rpls') }}


WITH rpls_commune AS (
   SELECT
       "DEPCOM_ARM" as com,
       densite,
       nb_ls,
       nb_loues,
       nb_vacants,
       nb_vides,
       nb_asso,
       nb_occup_finan,
       nb_occup_temp,
       nb_ls_en_qpv,
       tx_vac,
       tx_vac3,
       tx_mob
   FROM {{ ref('logement_rpls_commune') }}
),


communes as
(
   select
       code as code_com
   from {{ ref('geographical_references_communes') }}
)


SELECT
   *
FROM rpls_commune rpls
inner join communes com
       on rpls.com = com.code_com