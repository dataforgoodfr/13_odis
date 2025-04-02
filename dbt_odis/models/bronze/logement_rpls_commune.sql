{{ config(
    alias = 'vw_logement_rpls_commune'
    )
}}


WITH json_data AS (
    SELECT
        id,
        created_at,
        data
    FROM {{ source('bronze', 'logement_rpls_commune') }}
)

SELECT
    id,
    created_at,
    -- Utiliser le macro pour extraire dynamiquement les colonnes du JSON
    {{ flatten_json('data',source('bronze', 'logement_rpls_commune')) }}
FROM json_data
