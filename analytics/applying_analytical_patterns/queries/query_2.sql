WITH
  check_dupes AS (
    SELECT
      NGD.*,
      NG.home_team_id,
      NG.visitor_team_id,
      NG.home_team_wins,
      NG.season,
      ROW_NUMBER() OVER (
        PARTITION BY
          NGD.game_id,
          NGD.team_id,
          NGD.player_id
        ORDER BY
          NG.game_date_est
      ) AS row_num
    FROM
      bootcamp.nba_game_details NGD
      JOIN bootcamp.nba_games NG ON NGD.game_id = NG.game_id
  ),
  game_details_deduped AS (
    SELECT
      *
    FROM
      check_dupes
    WHERE
      row_num = 1
  )
SELECT
  team_id,
  team_abbreviation,
  player_id,
  player_name,
  season,
  COUNT(
    DISTINCT CASE
      WHEN home_team_id = team_id
      AND home_team_wins = 1 THEN game_id
      WHEN visitor_team_id = team_id
      AND home_team_wins = 0 THEN game_id
    END
  ) AS games_won,
  COALESCE(SUM(pts), 0) AS total_pts,
  COALESCE(SUM(fta), 0) AS total_fta,
  COALESCE(SUM(ftm), 0) AS total_ftm,
  COALESCE(SUM(fga), 0) AS total_fga,
  COALESCE(SUM(fgm), 0) AS total_fgm,
  COALESCE(ROUND(AVG(pts),2), 0) AS avg_pts,
  COALESCE(ROUND(AVG(fta),2), 0) AS avg_fta,
  COALESCE(ROUND(AVG(ftm),2), 0) AS avg_ftm,
  COALESCE(ROUND(AVG(fga),2), 0) AS avg_fga,
  COALESCE(ROUND(AVG(fgm),2), 0) AS avg_fgm
FROM
  game_details_deduped
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
