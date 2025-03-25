{{ config(
    alias = 'vw_services_services'
    )
}}


with services as 
(
    select 
        id as id, 
        json_value(data, '$.measures.OBS_VALUE_NIVEAU.value') as value, 
        json_value(data, '$.dimensions.GEO') as GEO, 
        json_value(data, '$.dimensions.BPE_MEASURE') as BPE_MEASURE,
        json_value(data, '$.dimensions.TIME_PERIOD') as TIME_PERIOD,
        json_value(data, '$.dimensions.FACILITY_DOM') as FACILITY_DOM,
        json_value(data, '$.dimensions.FACILITY_SDOM') as FACILITY_SDOM,
        json_value(data, '$.dimensions.FACILITY_TYPE') as FACILITY_TYPE,
        created_at as created_at
    from {{ source('bronze', 'services_services') }} 
)

select * from services
