-- dim_fire_incidents__location.sql
WITH location_distinct AS (
    SELECT DISTINCT
        address,
        city,
        supervisor_district,
        neighborhood_district,
        zipcode,
        coordinates
    FROM {{ ref('stg_fire_incidents__fire_incidents') }}
)

SELECT DISTINCT
    row_number() OVER () AS location_key,
    *
FROM location_distinct
