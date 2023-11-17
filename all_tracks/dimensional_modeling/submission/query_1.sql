CREATE TABLE stalwai.actors 
  ( actor varchar, 
    actor_id varchar,
    films array(ROW(film varchar, votes integer, rating double, film_id varchar)), 
    quality_class varchar,
    is_active boolean, 
    current_year integer ) 
  WITH ( format = 'PARQUET',  partitioning = ARRAY['current_year'] )
