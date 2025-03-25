{{ config(
    alias = 'vw_logements_appartement_et_residences_principales'
    )
}}


with appartement_rp as 
(
    select 
        id as id, 
        json_value(data, '$.measures.OBS_VALUE_NIVEAU.value') as value, 
        json_value(data, '$.dimensions.GEO') as GEO, 
        json_value(data, '$.dimensions.NOR') as NOR,
        json_value(data, '$.dimensions.OCS') as OCS,
        json_value(data, '$.dimensions.TDW') as TDW,
        json_value(data, '$.dimensions.TOH') as TOH,
        json_value(data, '$.dimensions.TSH') as TSH,
        json_value(data, '$.dimensions.CARS') as CARS,
        json_value(data, '$.dimensions.L_STAY') as L_STAY,
        json_value(data, '$.dimensions.CARPARK') as CARPARK,
        json_value(data, '$.dimensions.BUILD_END') as BUILD_END,      
        json_value(data, '$.dimensions.RP_MEASURE') as RP_MEASURE,
        json_value(data, '$.dimensions.TIME_PERIOD') as TIME_PERIOD,
        created_at as created_at
    from {{ source('bronze', 'logement_logements_appartement_et_residences_principales') }} 
)

select * from appartement_rp
