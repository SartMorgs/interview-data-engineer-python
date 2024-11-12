-- dim_fire_incidents__battalion.sql
WITH battalion_distinct AS (
  SELECT DISTINCT
    battalion,
    box,
    action_taken_primary,
    action_taken_secondary,
    primary_situation,
    property_use,
    station_area
  FROM {{ ref('stg_fire_incidents__fire_incidents') }}
)
SELECT
  row_number() OVER () AS battalion_key,
  *
FROM battalion_distinct