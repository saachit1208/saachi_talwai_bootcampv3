WITH
  check_dupes AS (
    SELECT
      NGD.*,
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
  )
SELECT
  game_id,
  team_id,
  team_abbreviation,
  team_city,
  player_id,
  player_name,
  nickname,
  start_position,
  COMMENT,
  MIN,
  fgm,
  fga,
  fg_pct,
  fg3m,
  fg3a,
  fg3_pct,
  ftm,
  fta,
  ft_pct,
  oreb,
  dreb,
  reb,
  ast,
  stl,
  blk,
  TO,
  pf,
  pts,
  plus_minus
FROM
  check_dupes
WHERE
  row_num = 1
