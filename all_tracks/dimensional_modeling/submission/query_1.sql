CREATE TABLE stalwai.actors 
  ( actor varchar, 
    actorid varchar,
    films array(ROW(film varchar, votes integer, rating double, filmid varchar)), 
    quality_class varchar,
    is_active boolean, 
    current_year integer ) 
  WITH ( format = 'PARQUET',  partitioning = ARRAY['current_year'] )
