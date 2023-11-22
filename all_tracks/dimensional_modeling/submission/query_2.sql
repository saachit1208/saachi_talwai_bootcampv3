/* Incrementally populating actors table */
/* Currently populated from 1914 to 1940 */
/* This query will populate it with year 1941 entries */

INSERT INTO stalwai.actors 
WITH
  distinct_years AS (
    SELECT
      yr
    FROM
      UNNEST (SEQUENCE(1914, 2021, 1)) AS t (yr)
  ),
  actor_min_years AS (
    SELECT
      actor_id,
      actor,
      MIN(YEAR) AS min_year
    FROM
      bootcamp.actor_films
    GROUP BY
      1,
      2
  ),
  actor_years AS (
    SELECT
      AM.actor_id,
      AM.actor,
      DY.yr
    FROM
      distinct_years DY
      JOIN actor_min_years AM ON DY.yr >= AM.min_year
    ORDER BY
      AM.actor,
      AM.actor_id,
      DY.yr
  ),
  actor_films_cte AS (
    SELECT
      AY.actor AS actor,
      AY.actor_id AS actor_id,
      AY.yr AS YEAR,
      CASE
        WHEN AVG(AF.rating) > 8 THEN 'star'
        WHEN AVG(AF.rating) > 7
        AND AVG(AF.rating) <= 8 THEN 'good'
        WHEN AVG(rating) > 6
        AND AVG(AF.rating) <= 7 THEN 'average'
        ELSE 'bad'
      END AS quality_class,
      COUNT(film) AS film_cnt,
      ARRAY_AGG(CAST(ROW (film, votes, rating, film_id) AS ROW(film VARCHAR, votes INTEGER, rating DOUBLE, filmid VARCHAR))) as films
    FROM
      actor_years AY
      LEFT JOIN bootcamp.actor_films AF ON AY.yr = AF.year
      AND AY.actor_id = AF.actor_id
      AND AY.actor = AF.actor
    GROUP BY
      AY.actor,
      AY.actor_id,
      AY.yr
    ORDER BY
      AY.yr
  ),
  last_year AS (
    SELECT
      *
    FROM
      stalwai.actors
    WHERE
      current_year = 1940
  ),

  this_year as (  SELECT
      *
    FROM
      actor_films_cte
    WHERE
      YEAR = 1941
  )
  SELECT
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actorid, ty.actor_id) AS actorid,
  CASE
    WHEN ty.film_cnt = 0 THEN ly.films
    WHEN ty.films IS NOT NULL
    AND ly.films IS NULL THEN ty.films
    WHEN ty.films IS NOT NULL
    AND ly.films IS NOT NULL THEN ty.films || ly.films
  END AS films,
   COALESCE(ly.quality_class, ty.quality_class) AS     quality_class,
  ty.film_cnt > 0 AS is_active,
  COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM
  last_year ly
  FULL OUTER JOIN this_year ty ON ly.actor = ty.actor AND ly.actorid = ty.actor_id
  

