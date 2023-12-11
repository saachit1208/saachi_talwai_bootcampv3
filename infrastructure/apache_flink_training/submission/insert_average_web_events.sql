INSERT INTO average_web_events 
SELECT host,  ROUND((SUM(num_hits)/COUNT(session_start)),2) 
FROM processed_events_aggregated
GROUP by host;
