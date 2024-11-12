SELECT 
    bat.battalion,
    loc.city,
    loc.neighborhood_district,
    COUNT(fireinc.incident_id) AS total_incidents,
    SUM(cast(fireinc.civilian_fatalities AS INTEGER)) AS total_civilian_fatalities,
    SUM(cast(fireinc.estimated_property_loss AS FLOAT)) AS total_property_loss
FROM 
    warehouse.fct_fire_incidents__fire_incidents fireinc
INNER JOIN
    dim_fire_incidents__battalion bat ON fireinc.battalion_key = bat.battalion_key
INNER JOIN 
    dim_fire_incidents__location loc ON fireinc.location_key = loc.location_key
GROUP BY 
    bat.battalion, loc.city, loc.neighborhood_district
ORDER BY 
    total_incidents DESC;