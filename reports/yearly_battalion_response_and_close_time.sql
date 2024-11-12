SELECT 
    datetime.incident_year,
    bat.battalion,
    AVG(fireinc.response_time) AS avg_response_time,
    AVG(fireinc.close_time) AS avg_close_time,
    SUM(cast(fireinc.suppression_units AS INTEGER)) AS total_suppression_units
FROM 
    warehouse.fct_fire_incidents__fire_incidents fireinc
INNER JOIN
    dim_fire_incidents__battalion bat ON fireinc.battalion_key = bat.battalion_key
INNER JOIN 
    warehouse.dim_fire_incidents__date datetime ON fireinc.incident_type_key = datetime.date_key
GROUP BY 
    datetime.incident_year, bat.battalion
ORDER BY 
    datetime.incident_year, bat.battalion;