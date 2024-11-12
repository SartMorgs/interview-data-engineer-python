-- fct_fire_incidents__fire_incidents.sql
WITH fire_incidents_cast AS (
    SELECT 
        incident_number,
        cast(incident_date AS TIMESTAMP) AS incident_date,
        cast(alarm_dttm AS TIMESTAMP) AS alarm_dttm,
        cast(arrival_dttm AS TIMESTAMP) AS arrival_dttm,
        cast(close_dttm AS TIMESTAMP) AS close_dttm,
        address,
        area_of_fire_origin,
        heat_source,
        ignition_cause,
        battalion,
        box,
        civilian_fatalities,
        civilian_injuries,
        ems_personnel,
        ems_units,
        estimated_contents_loss,
        estimated_property_loss,
        exposure_number,
        fire_fatalities,
        fire_injuries,
        number_of_alarms,
        other_personnel,
        other_units,
        suppression_units,
        suppression_personnel
    FROM {{ ref('stg_fire_incidents__fire_incidents') }}
),
base AS (
    SELECT
        fireinc.incident_number as incident_id,
        incident_date.date_key as incident_date_key,
        alarm_date.date_key as alarm_date_key,
        arrival_date.date_key as arrival_date_key,
        close_date.date_key as close_date_key,
        loc.location_key,
        typ.incident_type_key,
        bat.battalion_key,
        fireinc.civilian_fatalities,
        fireinc.civilian_injuries,
        fireinc.ems_personnel,
        fireinc.ems_units,
        fireinc.estimated_contents_loss,
        fireinc.estimated_property_loss,
        fireinc.exposure_number,
        fireinc.fire_fatalities,
        fireinc.fire_injuries,
        fireinc.number_of_alarms,
        fireinc.other_personnel,
        fireinc.other_units,
        fireinc.suppression_units,
        fireinc.suppression_personnel,
        (fireinc.arrival_dttm - fireinc.alarm_dttm) AS response_time,
        (fireinc.close_dttm - fireinc.arrival_dttm) AS close_time
    FROM fire_incidents_cast fireinc
    LEFT JOIN {{ ref('dim_fire_incidents__date') }} AS incident_date
        ON fireinc.incident_date = incident_date.incident_date
    LEFT JOIN {{ ref('dim_fire_incidents__date') }} AS alarm_date
        ON fireinc.alarm_dttm = alarm_date.alarm_dttm
    LEFT JOIN {{ ref('dim_fire_incidents__date') }} AS arrival_date
        ON fireinc.arrival_dttm = arrival_date.arrival_dttm
    LEFT JOIN {{ ref('dim_fire_incidents__date') }} AS close_date
        ON fireinc.close_dttm = close_date.close_dttm
    LEFT JOIN {{ ref('dim_fire_incidents__location') }} AS loc
        ON fireinc.address = loc.address
    LEFT JOIN {{ ref('dim_fire_incidents__incident_type') }} AS typ
        ON fireinc.area_of_fire_origin = typ.area_of_fire_origin
            AND fireinc.heat_source = typ.heat_source
            AND fireinc.ignition_cause = typ.ignition_cause
    LEFT JOIN {{ ref('dim_fire_incidents__battalion') }} AS bat
        ON fireinc.battalion = bat.battalion AND fireinc.box = bat.box
)

SELECT
    row_number() OVER () AS fire_incident_key,
    *
FROM base