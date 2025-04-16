{{ config(
    alias = 'vw_presentation_page_population_communes'
    )
}}


with population_communes as 
(
    select 
        id as id, 
        json_value(data, '$.measures.OBS_VALUE_NIVEAU.value') as value, 
        json_value(data, '$.dimensions.AGE') as AGE, 
        json_value(data, '$.dimensions.GEO') as GEO,
        json_value(data, '$.dimensions.SEX') as SEX,
        json_value(data, '$.dimensions.RP_MEASURE') as RP_MEASURE,
        json_value(data, '$.dimensions.TIME_PERIOD') as TIME_PERIOD,
        created_at as created_at
    from {{ source('bronze', 'presentation_page_population_communes') }} 
)

select * from population_communes
