/* Incrementally add data for year 1940 into actors_history_scd from actors table */
/* Done for year 1940 */

INSERT INTO stalwai.actors_history_scd
WITH
  last_year_scd AS (
    SELECT
      *
    FROM
      stalwai.actors_history_scd
    WHERE
      current_year = 1931
  ),
  current_year_scd AS (
    SELECT
      *
    FROM
      stalwai.actors
    WHERE
      current_year = 1932
  ),
  combined AS (
    SELECT
      COALESCE(ly.actor, cy.actor) AS actor,
      COALESCE(ly.actorid, cy.actorid) AS actorid,
      COALESCE(ly.start_date, cy.current_year) AS start_date,
      COALESCE(ly.end_date, cy.current_year) AS end_date,
      CASE
        WHEN ly.is_active <> cy.is_active OR ly.quality_class <> cy.quality_class THEN 1
        WHEN ly.is_active = cy.is_active  AND ly.quality_class = cy.quality_class THEN 0
      END AS did_change,
      cy.quality_class AS quality_class_this_year,
      ly.quality_class AS quality_class_last_year,
      ly.is_active AS is_active_last_year,
      cy.is_active AS is_active_this_year,
      1932 AS current_year
    FROM
      last_year_scd ly
      FULL OUTER JOIN current_year_scd cy ON ly.actor = cy.actor AND ly.actorid = cy.actorid AND ly.end_date + 1 = cy.current_year
  ),
  changes AS (
    SELECT
      actor,
      actorid,
      CASE
        WHEN did_change = 0 THEN ARRAY[
          CAST(
            ROW (
              quality_class_last_year,
              is_active_last_year,
              start_date,
              end_date + 1
            ) AS ROW (
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
        WHEN did_change = 1 THEN ARRAY[
          CAST(
            ROW (quality_class_last_year, is_active_last_year, start_date, end_date) AS ROW (
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          ),
          CAST(
            ROW (
              quality_class_this_year,
              is_active_this_year,
              current_year,
              current_year
            ) AS ROW (
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
        WHEN did_change IS NULL THEN ARRAY[
          CAST(
            ROW (
              COALESCE(quality_class_last_year, quality_class_this_year),
              COALESCE(is_active_last_year, is_active_this_year),
              start_date,
              end_date
            ) AS ROW (
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
      END AS change_array,
      current_year
    FROM
      combined
  )
SELECT
  actor,
  actorid,
  arr.quality_class,
  arr.is_active,
  arr.start_date,
  arr.end_date,
  current_year
FROM
  changes
  CROSS JOIN UNNEST (change_array) AS arr
