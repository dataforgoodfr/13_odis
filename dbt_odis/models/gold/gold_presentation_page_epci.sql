{{ config(
    tags = ['gold', 'homepage'],
    alias = 'gold_presentation_page_epci'
) }}

SELECT
    *
FROM {{ ref('silver_presentation_page_epci') }}