SELECT 
    bat.battalion,
    CASE 
        WHEN date_part('hour', datetime.alarm_dttm) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN date_part('hour', datetime.alarm_dttm) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN date_part('hour', datetime.alarm_dttm) BETWEEN 18 AND 23 THEN 'Evening'
        ELSE 'Night'
    END AS period,
    COUNT(fireinc.incident_id) AS total_incidents,
    SUM(cast(fireinc.suppression_personnel AS INTEGER)) AS total_suppression_personnel
FROM 
    warehouse.fct_fire_incidents__fire_incidents fireinc
INNER JOIN
    dim_fire_incidents__battalion bat ON fireinc.battalion_key = bat.battalion_key
INNER JOIN 
    warehouse.dim_fire_incidents__date datetime ON fireinc.alarm_date_key = datetime.date_key
GROUP BY 
    bat.battalion, period
ORDER BY 
    bat.battalion, period;