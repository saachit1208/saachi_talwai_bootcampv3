INSERT INTO stalwai.user_devices_cumulated
with  events_by_date AS (
    SELECT
      WE.user_id,
      D.browser_type,
      DATE(MAX(DATE_TRUNC('day', WE.event_time))) AS event_date
    FROM
      bootcamp.web_events WE
      JOIN bootcamp.devices D ON WE.device_id = D.device_id
    GROUP BY
      WE.user_id,
      D.browser_type,
      DATE_TRUNC('day', WE.event_time)
  ),
  yesterday AS (
    SELECT
      *
    FROM
      stalwai.user_devices_cumulated
    WHERE
      date = DATE('2021-01-07')
  ),
  today AS (
    SELECT
      *
    FROM
      events_by_date
    WHERE
      event_date = DATE('2021-01-08')
  )
SELECT
  COALESCE(y.user_id, t.user_id) AS user_id,
   COALESCE(y.browser_type, t.browser_type) AS browser_type,
  CASE
    WHEN y.dates_active IS NOT NULL THEN           
      ARRAY[t.event_date] ||  y.dates_active      
    ELSE
      ARRAY[t.event_date]
   END AS  dates_active,
  DATE('2021-01-08') AS date
FROM
  yesterday y
FULL OUTER JOIN 
  today t ON y.user_id = t.user_id
