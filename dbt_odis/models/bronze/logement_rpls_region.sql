{{ config(
    alias = 'vw_logement_rpls_region'
    )
}}


WITH json_data AS (
    SELECT
        id,
        created_at,
        data
    FROM {{ source('bronze', 'logement_rpls_region') }}
)

SELECT
    id,
    created_at,
    -- Utiliser le macro pour extraire dynamiquement les colonnes du JSON
    {{ flatten_json('data',source('bronze', 'logement_rpls_region')) }}
FROM json_data
