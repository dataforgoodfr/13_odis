{{ config(
    tags = ['bronze', 'population'],
    alias = 'vw_population_totale'
    )
}}

SELECT
	(data::jsonb)->'dimensions'->>'GEO'::VARCHAR AS geo,
    (data::jsonb)->'dimensions'->>'FREQ'::VARCHAR AS freq,
    ((data::jsonb)->'dimensions'->'TIME_PERIOD'->>0)::INTEGER AS time_period,
    (data::jsonb)->'dimensions'->>'POPREF_MEASURE'::VARCHAR AS popref_measure,
	((data::jsonb)->'measures'->'OBS_VALUE_NIVEAU'->'value'->>0)::DECIMAL AS value      
FROM {{ source('bronze', 'population_population_totale') }} 