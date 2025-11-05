{{ config(
    tags = ['silver', 'homepage'],
    alias = 'silver_presentation_page_epci'
) }}

WITH epci AS (
    SELECT
        code::int AS code,
        COUNT(*) AS nb_membres
    FROM {{ ref('presentation_page_epci') }}
    GROUP BY code
)
SELECT
    bronze_epci.*,
    epci.nb_membres,
    corresp_siren_codes_communes
FROM {{ ref('presentation_page_epci') }} bronze_epci
INNER JOIN epci
    ON epci.code = bronze_epci.code
INNER JOIN corresp_siren_codes_communes
    ON corresp_siren_codes_communes.SIREN = bronze_epci.code