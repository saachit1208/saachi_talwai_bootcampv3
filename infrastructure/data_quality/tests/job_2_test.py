from chispa.dataframe_comparer import *
from ..jobs.nba_game_details import nba_game_details_transformation
from collections import namedtuple

NBAGameDetails = namedtuple("NBAGameDetails", "game_id team_id team_abbreviation  player_id player_name pts home_team_id visitor_team_id home_team_wins season")
NbaGroupingAgg = namedtuple("NBAGroupinAgg", "team_id team_abbreviation	player_id player_name season games_won total_pts")

#Assuming Entries for Player - Brian Hamiliton with id = 201646
#The case where grouping set is team_id and team abbreviation assumes there is only one player in the team based on the source_data
def test_nba_grouping_sets(spark):
    source_data = [
    NBAGameDetails(10800098,1610612751,'NJN',201646,'Brian Hamilton', 0, 1610612751,1610612755,0,2008),
    NBAGameDetails(10800042,1610612751,'NJN',201646, 'Brian Hamilton', 1, 1610612751,1610612748,1,2008),
    NBAGameDetails(10900004,1610612751,'NJN',201646, 'Brian Hamilton', 0, 1610612752,1610612751,1,2009),
    NBAGameDetails(10800079, 1610612751,'NJN',201646, 'Brian Hamilton', 0, 1610612738, 1610612751, 1, 2008)
    ]
    source_df = spark.createDataFrame(source_data)
    actual_df = nba_game_details_transformation(spark, source_df)
    expected_data = [
       NbaGroupingAgg(None, None,201646,'Brian Hamilton',2008, 1, 1),
       NbaGroupingAgg(None, None,201646, 'Brian Hamilton',2009, 0, 0),
       NbaGroupingAgg(1610612751, 'NJN', None, None, None, 1, 1),
       NbaGroupingAgg(1610612751,'NJN',201646, 'Brian Hamilton', None, 1, 1)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

    