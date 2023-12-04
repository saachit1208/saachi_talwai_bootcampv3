/* Single backfill to load actors_history_scd till 1939 */
/* Did this because actors is loaded till 1940 */
/* Will incrementall add data for year 1940 in the next query (query 5) */

INSERT INTO stalwai.actors_history_scd
WITH
  lagged AS (
    SELECT
      actor,
      actorid,
      quality_class,
      CASE
        WHEN is_active THEN 1
        ELSE 0
      END AS is_active,
      LAG(quality_class, 1) OVER (
          PARTITION BY
            actor, actorid
          ORDER BY
            current_year
        ) AS quality_class_last_year, 
      CASE
        WHEN LAG(is_active, 1) OVER (
          PARTITION BY
            actor, actorid
          ORDER BY
            current_year
        ) THEN 1
        ELSE 0
      END AS is_active_last_year,
      current_year
    FROM
      stalwai.actors
    WHERE
      current_year <= 1939
  ),
  streaked AS (
    SELECT
      *,
      SUM(
        CASE
          WHEN is_active <> is_active_last_year OR quality_class <> quality_class_last_year THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          actor, actorid
        ORDER BY
          current_year
      ) AS streak_identifier
    FROM
      lagged
  )
SELECT
  actor,
  actorid,
  MAX(quality_class) as quality_class,
  MAX(is_active) = 1 AS is_active,
  MIN(current_year) AS start_year,
  MAX(current_year) AS end_year,
  1939 AS current_year
FROM
  streaked
GROUP BY
  actor,
  actorid,
  streak_identifier

