-- dim_fire_incidents__incident_type.sql
WITH incident_type_distinct AS (
    SELECT DISTINCT
        area_of_fire_origin,
        floor_of_fire_origin,
        heat_source,
        human_factors_associated_with_ignition,
        ignition_cause,
        ignition_factor_primary,
        item_first_ignited
    FROM {{ ref('stg_fire_incidents__fire_incidents') }}
)

SELECT DISTINCT
    row_number() OVER () AS incident_type_key,
    *
FROM incident_type_distinct
