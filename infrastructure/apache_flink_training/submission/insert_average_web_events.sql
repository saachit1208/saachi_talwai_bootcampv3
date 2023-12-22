INSERT INTO average_web_events 
SELECT host,  ROUND((AVG(num_hits)),2) 
FROM processed_events_aggregated
GROUP by host;
