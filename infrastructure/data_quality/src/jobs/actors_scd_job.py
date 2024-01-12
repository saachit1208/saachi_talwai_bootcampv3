from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


#Query 4 From HW 1 - Write a "backfill" query that can populate the entire actors_history_scd table in a single query.
def query_1():
    return f"""
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
          actors
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
      MAX(current_year) AS end_year
    FROM
      streaked
    GROUP BY
      actor,
      actorid,
      streak_identifier

    """

def actors_scd_transformation(spark, output_df: DataFrame) -> Optional[DataFrame]:
  print(output_df)
  output_df.createOrReplaceTempView("actors")
  return spark.sql(query_1())

def main():
    output_table_name: str = "actors_scd"
    spark: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("actors_scd")
        .getOrCreate()
    )
    output_df = actors_scd_transformation(spark, spark.table("actors"))
    output_df.write.mode("overwrite").insertInto(output_table_name)