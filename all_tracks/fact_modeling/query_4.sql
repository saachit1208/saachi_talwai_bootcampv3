CREATE TABLE
  stalwai.hosts_cumulated (
    host VARCHAR,
    host_activity_datelist ARRAY (DATE),
    date DATE
  )
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['date']
  )
