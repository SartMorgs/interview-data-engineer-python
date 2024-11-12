SELECT 
    datetime.incident_year,
    datetime.incident_month,
    COUNT(fireinc.incident_id) AS total_incidents,
    SUM(CAST(fireinc.civilian_fatalities AS INTEGER)) AS total_civilian_fatalities,
    SUM(CAST(fireinc.estimated_property_loss AS FLOAT)) AS total_property_loss
FROM 
    warehouse.fct_fire_incidents__fire_incidents fireinc
INNER JOIN 
    warehouse.dim_fire_incidents__date datetime ON fireinc.incident_type_key = datetime.date_key
GROUP BY 
    datetime.incident_year, datetime.incident_month
ORDER BY 
    datetime.incident_year, datetime.incident_month;