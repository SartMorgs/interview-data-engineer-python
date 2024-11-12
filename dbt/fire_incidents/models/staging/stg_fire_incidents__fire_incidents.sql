-- stg_fire_incidents__fire_incidents.sql
SELECT
    *
FROM {{ source('fire_incidents', 'fire_incidents') }}