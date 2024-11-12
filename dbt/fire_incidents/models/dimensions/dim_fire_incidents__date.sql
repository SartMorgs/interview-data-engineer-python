-- dim_fire_incidents__date.sql
WITH incident_date_cast AS (
    SELECT
        cast(incident_date AS TIMESTAMP) AS incident_date,
        cast(alarm_dttm AS TIMESTAMP) AS alarm_dttm,
        cast(arrival_dttm AS TIMESTAMP) AS arrival_dttm,
        cast(close_dttm AS TIMESTAMP) AS close_dttm
    FROM {{ ref('stg_fire_incidents__fire_incidents') }}
),
date_components  AS (
    SELECT
        incident_date,
        date_part('year', incident_date) AS incident_year,
        date_part('month', incident_date) AS incident_month,
        date_part('day', incident_date) AS incident_day,
        date_part('dow', incident_date) AS incident_day_of_week,
        alarm_dttm,
        date_part('year', alarm_dttm) AS alarm_year,
        date_part('month', alarm_dttm) AS alarm_month,
        date_part('day', alarm_dttm) AS alarm_day,
        date_part('dow', alarm_dttm) AS alarm_day_of_week,
        arrival_dttm,
        date_part('year', arrival_dttm) AS arrival_year,
        date_part('month', arrival_dttm) AS arrival_month,
        date_part('day', arrival_dttm) AS arrival_day,
        date_part('dow', arrival_dttm) AS arrival_day_of_week,
        close_dttm,
        date_part('year', close_dttm) AS close_year,
        date_part('month', close_dttm) AS close_month,
        date_part('day', close_dttm) AS close_day,
        date_part('dow', close_dttm) AS close_day_of_week
    FROM incident_date_cast
)

SELECT
    row_number() OVER () AS date_key,
    *
FROM date_components