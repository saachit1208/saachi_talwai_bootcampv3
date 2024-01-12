from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

#Query 2 From HW 3
# Write a query (query_2) that uses GROUPING SETS to perform aggregations of the game_details data. Create slices that aggregate along the following combinations of dimensions:
# player and team
# player and season
# team
#Assume table is joined and deduped 

def query_2():
    return f"""
  WITH
  game_details_deduped_joined AS (
    SELECT
      NGD.game_id,
      NGD.team_id,
      NGD.team_abbreviation,
      NGD.player_id,
      NGD.player_name,
      NGD.pts,
      NGD.season,
      NGD.home_team_id,
      NGD.visitor_team_id,
      NGD.home_team_wins,
      NGD.season
    FROM
      nba_game_details_deduped_joined NGD
  )
SELECT
  team_id,
  team_abbreviation,
  player_id,
  player_name,
  season,
  COUNT (
  DISTINCT  CASE
      WHEN home_team_id = team_id
      AND home_team_wins = 1 THEN game_id
      WHEN visitor_team_id = team_id
      AND home_team_wins = 0 THEN game_id
    END
  ) AS games_won,
  COALESCE(SUM(pts), 0) AS total_pts
FROM
  game_details_deduped_joined
GROUP BY
  GROUPING SETS (
    (
      team_id,
      team_abbreviation,
      player_id,
      player_name
    ),
    (player_id, player_name, season),
    (team_id, team_abbreviation)
  )
ORDER BY
  team_id,
  team_abbreviation,
  player_id,
  player_name,
  season
"""

def nba_game_details_transformation(spark, output_df: DataFrame) -> Optional[DataFrame]:
  output_df.createOrReplaceTempView("nba_game_details_deduped_joined")
  return spark.sql(query_2())

def main():
    output_table_name: str = "nba_"
    spark: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("nba_grouping_sets")
        .getOrCreate()
    )
    output_df = nba_game_details_transformation(spark, spark.table("nba_game_details_deduped_joined"))
    output_df.write.mode("overwrite").insertInto("nba_game_agg")