{{ config(
    tags = ['bronze', 'presentation'],
    alias = 'vw_presentation_page_epci'
    )
}}

SELECT
    data->'nom'::text AS nom,
    -- (data::jsonb)->'nom'::text AS nom,
    (data::jsonb)->'code'->>0::int AS code,
    (data::jsonb)->'population'->>0::int AS population,
    (data::jsonb)->'codesRegions'->>0 AS codesRegions,
    (data::jsonb)->'codesDepartements'->>0 AS codesDepartements,
    created_at
FROM {{ ref('geographical_references_epcis') }}