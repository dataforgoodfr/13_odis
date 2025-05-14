{{ config(
    tags = ['bronze', 'services'],
    alias = 'vw_services_services'
    )
}}


with services as 
(
    select 
        id, 
        (data::jsonb)-> 'measures' -> 'OBS_VALUE_NIVEAU' ->> 'value' as value, 
        (data::jsonb)-> 'dimensions' ->> 'GEO' as GEO, 
        (data::jsonb)-> 'dimensions' ->> 'BPE_MEASURE' as BPE_MEASURE,
        (data::jsonb)-> 'dimensions' ->> 'TIME_PERIOD' as TIME_PERIOD,
        (data::jsonb)-> 'dimensions' ->> 'FACILITY_DOM' as FACILITY_DOM,
        (data::jsonb)-> 'dimensions' ->> 'FACILITY_SDOM' as FACILITY_SDOM,
        (data::jsonb)-> 'dimensions' ->> 'FACILITY_TYPE' as FACILITY_TYPE,
        created_at
    from {{ source('bronze', 'services_services') }} 
)

select * from services
