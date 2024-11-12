SELECT 
    datetime.incident_day_of_week AS day_of_week,
    COUNT(fireinc.incident_id) AS total_incidents,
    AVG(CAST(fireinc.estimated_property_loss AS FLOAT)) AS avg_property_loss
FROM 
    warehouse.fct_fire_incidents__fire_incidents fireinc
INNER JOIN 
    warehouse.dim_fire_incidents__date datetime ON fireinc.incident_date_key = datetime.date_key
GROUP BY 
    datetime.incident_day_of_week
ORDER BY 
    total_incidents DESC;