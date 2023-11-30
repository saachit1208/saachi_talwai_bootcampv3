INSERT INTO
  stalwai.hosts_cumulated
WITH
  events_by_date AS (
    SELECT
      WE.host,
      DATE(MAX(DATE_TRUNC('day', WE.event_time))) AS event_date
    FROM
      bootcamp.web_events WE
    GROUP BY
      WE.host,
      DATE_TRUNC('day', WE.event_time)
  ),
  yesterday AS (
    SELECT
      *
    FROM
      stalwai.hosts_cumulated
    WHERE
      DATE = DATE('2021-01-07')
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
  COALESCE(y.host, t.host) AS host,
  CASE
    WHEN y.host_activity_datelist IS NOT NULL THEN ARRAY[t.event_date] || y.host_activity_datelist
    ELSE ARRAY[t.event_date]
  END AS host_activity_datelist,
  DATE('2021-01-08') AS DATE
FROM
  yesterday y
  FULL OUTER JOIN today t ON y.host = t.host
