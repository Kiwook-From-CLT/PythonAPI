
# coding: utf-8
# Author: Kiwook.K
# Pulling NBA players' data from NBA.com using NBA_py and pushing them into database
# 
import sys
import pandas as pd
import json, ijson
import requests, base64
from pandas.io.json import json_normalize
import nba_py
from nba_py import *
from nba_py import _api_scrape, _get_json, HAS_PANDAS, league, player
from nba_py.constants import *
import sqlalchemy as sqlal
import time


# Set a table name in MySQL database to insert data.
# Please check the index below CAREFULLY.

def get_table_name(argument):
    switcher = {
        1: "player_info",
        2: "player_stat",
        3: "player_location",
        4: "player_win_losses",
        5: "player_month",
        6: "player_pre_post_all_star",
        7: "player_starting_position",
        8: "player_days_rest",
        9: "player_by_conference",
        10: "player_by_division",
        11: "player_by_opponent",
        12: "player_last5",
        13: "player_last10",
        14: "player_last15",
        15: "player_last20",
        16: "player_gamenumber",
        17: "player_by_half",
        18: "player_by_period",
        19: "player_by_margin",
        20: "player_by_actual_margin",
        21: "player_last5min_deficit_5point",
        22: "player_last3min_deficit_5point",
        23: "player_last1min_deficit_5point",
        24: "player_last30sec_deficit_3point",
        25: "player_last10sec_deficit_3point",
        26: "player_last5min_plusminus_5point",
        27: "player_last3min_plusminus_5point",
        28: "player_last1min_plusminus_5point",
        29: "player_last30sec_plusminus_5point",
        30: "player_shot_5ft",
        31: "player_shot_8ft",
        32: "player_shot_areas",
        33: "player_assisted_shots",
        34: "player_shot_types_summary",
        35: "player_shot_types_detail",
        36: "player_assissted_by",
        37: "player_score_differential",
        38: "player_points_scored",
        39: "player_points_against",
        40: "player_by_year",
        41: "player_regular_season_totals",
        42: "player_regular_season_career_totals",
        43: "player_post_season_career_totals",
        44: "player_all_star_season_totals",
        45: "player_career_all_star_season_totals",
        46: "player_college_season_totals",
        47: "player_college_season_career_totals",
        48: "player_regular_season_rankings",
        49: "player_post_season_rankings",
        50: "player_season_highs",
        51: "player_career_highs",
        52: "player_next_game",
        53: "player_game_logs",
        54: "player_general_shooting",
        55: "player_shot_clock_shooting",
        56: "player_dribble_shooting",
        57: "player_closest_defender_shooting",
        58: "player_closest_defender_shooting_long",
        59: "player_touch_time_shooting",
        60: "player_shot_type_rebounding",
        61: "player_num_contested_rebounding",
        62: "player_shot_distance_rebounding",
        63: "player_rebound_distance_rebounding",
        64: "player_passes_made",
        65: "player_passes_recieved"
        
    }
    result = switcher.get(argument, "Invalid argument")
    return result


# Get player ID, Team ID, and playing season from the data. These will be primary keys 

def get_player_IDs(season_year):
    l = league.PlayerStats(season=season_year)
    df = l.overall()
    player_id = ({"Player_ID":df.PLAYER_ID,"Team_ID":df.TEAM_ID,"Season":season_year})
    
    return player_id


# Setup sleep. If there are too much requests to pull, server(NBA.com) won't response so that I should make some delays between every requests

recorded_time = None
def sleep_if_needed():
    global recorded_time
    time_now = time.time()

    if recorded_time:
        time_delta = time_now - recorded_time
        rand_sleep = 1.0
        if time_delta < rand_sleep:
            sleep = rand_sleep - time_delta
            #print(f'Sleeping for {sleep} secs')
            time.sleep(sleep)

    recorded_time = time_now


# Adding primary keys to every dataframes
def add_primary_key(df_player, PID, TID, season_year):
    
    df_player['PLAYER_ID'] = PID
    df_player['TEAM_ID'] = TID
    df_player['Season'] = season_year
    
    return df_player
    
    
# Functions start with "get_" collect raw data from NBA.com and calling fucntion that pushes data into database 
def get_player_Summary(PID, TID, SID):
    
    player_info = player.PlayerSummary(PID).info()
    
    if player_info.dropna().empty == False: 
        push_player_data(player_info, 1)
    if SID == '2017-18':
        player_stat = player.PlayerSummary(PID).headline_stats()
        player_stat['TEAM_ID'] = player_info['TEAM_ID']
        if player_stat.dropna().empty == False:
            push_player_data(player_stat, 2)
    
    
def get_player_GeneralSplits(PID, TID, season_year):
    
    player_location = player.PlayerGeneralSplits(PID, season=season_year).location()
    player_win_losses = player.PlayerGeneralSplits(PID, season=season_year).win_losses()
    player_month = player.PlayerGeneralSplits(PID, season=season_year).month()
    player_pre_post_all_star = player.PlayerGeneralSplits(PID, season=season_year).pre_post_all_star()
    player_starting_position = player.PlayerGeneralSplits(PID, season=season_year).starting_position()
    player_days_rest = player.PlayerGeneralSplits(PID, season=season_year).days_rest()
    
    
    if player_location.dropna().empty == False: 
        player_location = add_primary_key (player_location, PID, TID, season_year)
        push_player_data(player_location, 3)

    if player_win_losses.dropna().empty == False:
        player_win_losses = add_primary_key (player_win_losses, PID, TID, season_year)
        push_player_data(player_win_losses, 4)
    
    if player_month.dropna().empty == False:
        player_month = add_primary_key (player_month, PID, TID, season_year)
        push_player_data(player_month, 5)
    
    if player_pre_post_all_star.dropna().empty == False:
        player_pre_post_all_star = add_primary_key (player_pre_post_all_star, PID, TID, season_year)
        push_player_data(player_pre_post_all_star, 6)
    
    if player_starting_position.dropna().empty == False:
        player_starting_position = add_primary_key (player_starting_position, PID, TID, season_year)
        push_player_data(player_starting_position, 7)
    
    if player_days_rest.dropna().empty == False:
        player_days_rest = add_primary_key (player_days_rest, PID, TID, season_year)
        push_player_data(player_days_rest, 8)
    

def get_player_OpponentSplits(PID, TID, season_year):
    
    player_by_conference = player.PlayerOpponentSplits(PID, season=season_year).by_conference()
    player_by_division = player.PlayerOpponentSplits(PID, season=season_year).by_division()
    player_by_opponent = player.PlayerOpponentSplits(PID, season=season_year).by_opponent()
        
    if player_by_conference.dropna().empty == False: 
        player_by_conference = add_primary_key (player_by_conference, PID, TID, season_year)
        push_player_data(player_by_conference, 9)

    if player_by_division.dropna().empty == False:
        player_by_division = add_primary_key (player_by_division, PID, TID, season_year)
        push_player_data(player_by_division, 10)
    
    if player_by_opponent.dropna().empty == False:
        player_by_opponent = add_primary_key (player_by_opponent, PID, TID, season_year)
        push_player_data(player_by_opponent, 11)



def get_player_LastNGamesSplits(PID, TID, season_year):
    
    player_last5 = player.PlayerLastNGamesSplits(PID, season=season_year).last5()
    player_last10 = player.PlayerLastNGamesSplits(PID, season=season_year).last10()
    player_last15 = player.PlayerLastNGamesSplits(PID, season=season_year).last15()
    player_last20 = player.PlayerLastNGamesSplits(PID, season=season_year).last20()
    player_gamenumber = player.PlayerLastNGamesSplits(PID, season=season_year).gamenumber()
    
    if player_last5.dropna().empty == False: 
        player_last5 = add_primary_key (player_last5, PID, TID, season_year)
        push_player_data(player_last5, 12)

    if player_last10.dropna().empty == False:
        player_last10 = add_primary_key (player_last10, PID, TID, season_year)
        push_player_data(player_last10, 13)
    
    if player_last15.dropna().empty == False:
        player_last15 = add_primary_key (player_last15, PID, TID, season_year)
        push_player_data(player_last15, 14)
    
    if player_last20.dropna().empty == False:
        player_last20 = add_primary_key (player_last20, PID, TID, season_year)
        push_player_data(player_last20, 15)
        
    if player_gamenumber.dropna().empty == False:
        player_gamenumber = add_primary_key (player_gamenumber, PID, TID, season_year)
        push_player_data(player_gamenumber, 16)



def get_player_InGameSplits(PID, TID, season_year):
    
    player_by_half = player.PlayerInGameSplits(PID, season=season_year).by_half()
    player_by_period = player.PlayerInGameSplits(PID, season=season_year).by_period()
    player_by_score_margin = player.PlayerInGameSplits(PID, season=season_year).by_score_margin()
    player_by_actual_margin = player.PlayerInGameSplits(PID, season=season_year).by_actual_margin()
    
    if player_by_half.dropna().empty == False: 
        player_by_half = add_primary_key (player_by_half, PID, TID, season_year)
        push_player_data(player_by_half, 17)

    if player_by_period.dropna().empty == False:
        player_by_period = add_primary_key (player_by_period, PID, TID, season_year)
        push_player_data(player_by_period, 18)
    
    if player_by_score_margin.dropna().empty == False:
        player_by_score_margin = add_primary_key (player_by_score_margin, PID, TID, season_year)
        push_player_data(player_by_score_margin, 19)
    
    if player_by_actual_margin.dropna().empty == False:
        player_by_actual_margin = add_primary_key (player_by_actual_margin, PID, TID, season_year)
        push_player_data(player_by_actual_margin, 20)
        


def get_player_ClutchSplits(PID, TID, season_year):
 
    player_last5min_deficit_5point = player.PlayerClutchSplits(PID, season=season_year).last5min_deficit_5point()
    player_last3min_deficit_5point = player.PlayerClutchSplits(PID, season=season_year).last3min_deficit_5point()
    player_last1min_deficit_5point = player.PlayerClutchSplits(PID, season=season_year).last1min_deficit_5point()
    player_last30sec_deficit_3point = player.PlayerClutchSplits(PID, season=season_year).last30sec_deficit_3point()
    player_last10sec_deficit_3point = player.PlayerClutchSplits(PID, season=season_year).last10sec_deficit_3point()
    player_last5min_plusminus_5point = player.PlayerClutchSplits(PID, season=season_year).last5min_plusminus_5point()
    player_last3min_plusminus_5point = player.PlayerClutchSplits(PID, season=season_year).last3min_plusminus_5point()
    player_last1min_plusminus_5point = player.PlayerClutchSplits(PID, season=season_year).last1min_plusminus_5point()
    player_last30sec_plusminus_5point = player.PlayerClutchSplits(PID, season=season_year).last30sec_plusminus_5point()
    
    if player_last5min_deficit_5point.dropna().empty == False: 
        player_last5min_deficit_5point = add_primary_key (player_last5min_deficit_5point, PID, TID, season_year)
        push_player_data(player_last5min_deficit_5point, 21)

    if player_last3min_deficit_5point.dropna().empty == False:
        player_last3min_deficit_5point = add_primary_key (player_last3min_deficit_5point, PID, TID, season_year)
        push_player_data(player_last3min_deficit_5point, 22)
    
    if player_last1min_deficit_5point.dropna().empty == False:
        player_last1min_deficit_5point = add_primary_key (player_last1min_deficit_5point, PID, TID, season_year)
        push_player_data(player_last1min_deficit_5point, 23)
    
    if player_last30sec_deficit_3point.dropna().empty == False:
        player_last30sec_deficit_3point = add_primary_key (player_last30sec_deficit_3point, PID, TID, season_year)
        push_player_data(player_last30sec_deficit_3point, 24)
    
    if player_last10sec_deficit_3point.dropna().empty == False: 
        player_last10sec_deficit_3point = add_primary_key (player_last10sec_deficit_3point, PID, TID, season_year)
        push_player_data(player_last10sec_deficit_3point, 25)

    if player_last5min_plusminus_5point.dropna().empty == False:
        player_last5min_plusminus_5point = add_primary_key (player_last5min_plusminus_5point, PID, TID, season_year)
        push_player_data(player_last5min_plusminus_5point, 26)
    
    if player_last3min_plusminus_5point.dropna().empty == False:
        player_last3min_plusminus_5point = add_primary_key (player_last3min_plusminus_5point, PID, TID, season_year)
        push_player_data(player_last3min_plusminus_5point, 27)
    
    if player_last1min_plusminus_5point.dropna().empty == False:
        player_last1min_plusminus_5point = add_primary_key (player_last1min_plusminus_5point, PID, TID, season_year)
        push_player_data(player_last1min_plusminus_5point, 28)
    
    if player_last30sec_plusminus_5point.dropna().empty == False:
        player_last30sec_plusminus_5point = add_primary_key (player_last30sec_plusminus_5point, PID, TID, season_year)
        push_player_data(player_last30sec_plusminus_5point, 29)


def get_player_ShootingSplits(PID, TID, season_year):
      
    player_shot_5ft = player.PlayerShootingSplits(PID, season=season_year).shot_5ft()
    player_shot_8ft = player.PlayerShootingSplits(PID, season=season_year).shot_8ft()
    player_shot_areas = player.PlayerShootingSplits(PID, season=season_year).shot_areas()
    player_assisted_shots = player.PlayerShootingSplits(PID, season=season_year).assisted_shots()
    player_shot_types_summary = player.PlayerShootingSplits(PID, season=season_year).shot_types_summary()
    player_shot_types_detail = player.PlayerShootingSplits(PID, season=season_year).shot_types_detail()
    player_assissted_by = player.PlayerShootingSplits(PID, season=season_year).assissted_by()
    
    
    if player_shot_5ft.dropna().empty == False: 
        player_shot_5ft = add_primary_key (player_shot_5ft, PID, TID, season_year)
        push_player_data(player_shot_5ft, 30)

    if player_shot_8ft.dropna().empty == False:
        player_shot_8ft = add_primary_key (player_shot_8ft, PID, TID, season_year)
        push_player_data(player_shot_8ft, 31)
    
    if player_shot_areas.dropna().empty == False:
        player_shot_areas = add_primary_key (player_shot_areas, PID, TID, season_year)
        push_player_data(player_shot_areas, 32)
    
    if player_assisted_shots.dropna().empty == False:
        player_assisted_shots = add_primary_key (player_assisted_shots, PID, TID, season_year)
        push_player_data(player_assisted_shots, 33)
    
    if player_shot_types_summary.dropna().empty == False:
        player_shot_types_summary = add_primary_key (player_shot_types_summary, PID, TID, season_year)
        push_player_data(player_shot_types_summary, 34)
    
    if player_shot_types_detail.dropna().empty == False:
        player_shot_types_detail = add_primary_key (player_shot_types_detail, PID, TID, season_year)
        push_player_data(player_shot_types_detail, 35)
    
    if player_assissted_by.dropna().empty == False:
        player_assissted_by = add_primary_key (player_assissted_by, PID, TID, season_year)
        push_player_data(player_assissted_by, 36)


def get_player_PerformanceSplits(PID, TID, season_year):

    player_score_differential = player.PlayerPerformanceSplits(PID, season=season_year).score_differential()
    player_points_scored = player.PlayerPerformanceSplits(PID, season=season_year).points_scored()
    player_points_against = player.PlayerPerformanceSplits(PID, season=season_year).points_against()
    
    if player_score_differential.dropna().empty == False: 
        player_score_differential = add_primary_key (player_score_differential, PID, TID, season_year)
        push_player_data(player_score_differential, 37)

    if player_points_scored.dropna().empty == False:
        player_points_scored = add_primary_key (player_points_scored, PID, TID, season_year)
        push_player_data(player_points_scored, 38)
    
    if player_points_against.dropna().empty == False:
        player_points_against = add_primary_key (player_points_against, PID, TID, season_year)
        push_player_data(player_points_against, 39)
    


def get_player_YearOverYearSplits(PID, TID, season_year):
    
    player_by_year = player.PlayerYearOverYearSplits(PID, season=season_year).by_year()
   
    if player_by_year.dropna().empty == False: 
        player_by_year = add_primary_key (player_by_year, PID, TID, season_year)
        push_player_data(player_by_year, 40)



def get_player_Career(PID, TID, season_year):

    player_regular_season_totals = player.PlayerCareer(PID).regular_season_totals()
    player_regular_season_career_totals = player.PlayerCareer(PID).regular_season_career_totals()
    player_post_season_career_totals = player.PlayerCareer(PID).post_season_career_totals()
    player_all_star_season_totals = player.PlayerCareer(PID).all_star_season_totals()
    player_career_all_star_season_totals = player.PlayerCareer(PID).career_all_star_season_totals()
    player_college_season_totals = player.PlayerCareer(PID).college_season_totals()
    player_college_season_career_totals = player.PlayerCareer(PID).college_season_career_totals()
    player_regular_season_rankings = player.PlayerCareer(PID).regular_season_rankings()
    player_post_season_rankings = player.PlayerCareer(PID).post_season_rankings()
    
    if player_regular_season_totals.dropna().empty == False: 
        push_player_data(player_regular_season_totals, 41)

    if player_regular_season_career_totals.dropna().empty == False:
        push_player_data(player_regular_season_career_totals, 42)
    
    if player_post_season_career_totals.dropna().empty == False:
        push_player_data(player_post_season_career_totals, 43)
    
    if player_all_star_season_totals.dropna().empty == False:
        push_player_data(player_all_star_season_totals, 44)
    
    if player_career_all_star_season_totals.dropna().empty == False:
        push_player_data(player_career_all_star_season_totals, 45)
   
    if player_college_season_totals.dropna().empty == False:
        push_player_data(player_college_season_totals, 46)
    
    if player_college_season_career_totals.dropna().empty == False:
        push_player_data(player_college_season_career_totals, 47)
    
    if player_regular_season_rankings.dropna().empty == False:
        push_player_data(player_regular_season_rankings, 48)
    
    if player_post_season_rankings.dropna().empty == False:
        push_player_data(player_post_season_rankings, 49)



def get_player_Profile(PID, TID, season_year):
    
    player_season_highs = player.PlayerProfile(PID).season_highs()
    player_career_highs = player.PlayerProfile(PID).career_highs()
    player_next_game = player.PlayerProfile(PID).next_game()
   
    if player_season_highs.dropna().empty == False: 
        push_player_data(player_season_highs, 50)

    if player_career_highs.dropna().empty == False: 
        push_player_data(player_career_highs, 51)

    if player_next_game.dropna().empty == False: 
        push_player_data(player_next_game, 52)


def get_player_ShotTraking(PID, TID, season_year):
    
    player_general_shooting = player.PlayerShotTracking(PID, season=season_year).general_shooting()
    player_shot_clock_shooting = player.PlayerShotTracking(PID, season=season_year).shot_clock_shooting()
    player_dribble_shooting = player.PlayerShotTracking(PID, season=season_year).dribble_shooting()
    player_closest_defender_shooting = player.PlayerShotTracking(PID, season=season_year).closest_defender_shooting()
    player_closest_defender_shooting_long = player.PlayerShotTracking(PID, season=season_year).closest_defender_shooting_long()
    player_touch_time_shooting = player.PlayerShotTracking(PID, season=season_year).touch_time_shooting()
   
    if player_general_shooting.dropna().empty == False: 
        player_general_shooting = add_primary_key (player_general_shooting, PID, TID, season_year)
        push_player_data(player_general_shooting, 54)
    
    if player_shot_clock_shooting.dropna().empty == False: 
        player_shot_clock_shooting = add_primary_key (player_shot_clock_shooting, PID, TID, season_year)
        push_player_data(player_shot_clock_shooting, 55)
    
    if player_dribble_shooting.dropna().empty == False: 
        player_dribble_shooting = add_primary_key (player_dribble_shooting, PID, TID, season_year)
        push_player_data(player_dribble_shooting, 56)
    
    if player_closest_defender_shooting.dropna().empty == False: 
        player_closest_defender_shooting = add_primary_key (player_closest_defender_shooting, PID, TID, season_year)
        push_player_data(player_closest_defender_shooting, 57)
    
    if player_closest_defender_shooting_long.dropna().empty == False: 
        player_closest_defender_shooting_long = add_primary_key (player_closest_defender_shooting_long, PID, TID, season_year)
        push_player_data(player_closest_defender_shooting_long, 58)
    
    if player_touch_time_shooting.dropna().empty == False: 
        player_touch_time_shooting = add_primary_key (player_touch_time_shooting, PID, TID, season_year)
        push_player_data(player_touch_time_shooting, 59)    

        
def get_player_ReboundTracking(PID, TID, season_year):
    
    player_shot_type_rebounding = player.PlayerReboundTracking(PID, season=season_year).shot_type_rebounding()
    player_num_contested_rebounding = player.PlayerReboundTracking(PID, season=season_year).num_contested_rebounding()
    player_shot_distance_rebounding = player.PlayerReboundTracking(PID, season=season_year).shot_distance_rebounding()
    player_rebound_distance_rebounding = player.PlayerReboundTracking(PID, season=season_year).rebound_distance_rebounding()
    
    if player_shot_type_rebounding.dropna().empty == False: 
        player_shot_type_rebounding = add_primary_key (player_shot_type_rebounding, PID, TID, season_year)
        push_player_data(player_shot_type_rebounding, 60)
    
    if player_num_contested_rebounding.dropna().empty == False: 
        player_num_contested_rebounding = add_primary_key (player_num_contested_rebounding, PID, TID, season_year)
        push_player_data(player_num_contested_rebounding, 61)
    
    if player_shot_distance_rebounding.dropna().empty == False: 
        player_shot_distance_rebounding = add_primary_key (player_shot_distance_rebounding, PID, TID, season_year)
        push_player_data(player_shot_distance_rebounding, 62)
    
    if player_rebound_distance_rebounding.dropna().empty == False: 
        player_rebound_distance_rebounding = add_primary_key (player_rebound_distance_rebounding, PID, TID, season_year)
        push_player_data(player_rebound_distance_rebounding, 63)
    


def get_player_PassTracking(PID, TID, season_year):
    
    player_passes_made = player.PlayerPassTracking(PID, season=season_year).passes_made()
    player_passes_recieved = player.PlayerPassTracking(PID, season=season_year).passes_recieved()
   
    if player_passes_made.dropna().empty == False: 
        player_passes_made = add_primary_key (player_passes_made, PID, TID, season_year)
        push_player_data(player_passes_made, 64)
    
    if player_passes_recieved.dropna().empty == False: 
        player_passes_recieved = add_primary_key (player_passes_recieved, PID, TID, season_year)
        push_player_data(player_passes_recieved, 65)

        
# Push the dataframe into database
def push_player_data(df_pushing, flag):
    
    # Get table name to insert data
    table_name = get_table_name(flag)
    
    trans = conn.begin()
    try:
        df_pushing.to_sql(name = table_name, con = conn, if_exists='append', index=False, index_label=None, chunksize=None)
        trans.commit()
    except:
        trans.rollback()
        print(str(e), ' error occured. Data will not be inserted.')
        raise
    
    trans.close() 

# Pulling current season's data
def pull_current(arg):
    l = league.PlayerStats(season=arg)
    df = l.overall()

    player_id = pd.DataFrame(({"PLAYER_ID":df.PLAYER_ID,"TEAM_ID":df.TEAM_ID,"Season":arg}))

    # DB connection open (Have to change database URL)
    engine = sqlal.create_engine('YOU SHOULD CHANGE HERE TO YOUR DATABASE URL')
    conn = engine.connect()

    start_time = time.time()

    for idx, IDs in player_id.iterrows():
        try:
            get_player_Summary(IDs['PLAYER_ID'], IDs['TEAM_ID'], IDs['Season'])
        except:
            sleep_if_needed()
        try:
            get_player_GeneralSplits(IDs['PLAYER_ID'], IDs['TEAM_ID'], IDs['Season'])
        except:
            sleep_if_needed()
        try:
            get_player_OpponentSplits(IDs['PLAYER_ID'], IDs['TEAM_ID'],IDs['Season'])
        except:
            sleep_if_needed()
        try:
            get_player_LastNGamesSplits(IDs['PLAYER_ID'], IDs['TEAM_ID'],IDs['Season'])
        except:
            sleep_if_needed()
        try:
            get_player_InGameSplits(IDs['PLAYER_ID'], IDs['TEAM_ID'],IDs['Season'])
        except:
            sleep_if_needed()
        try:
            get_player_ClutchSplits(IDs['PLAYER_ID'], IDs['TEAM_ID'],IDs['Season'])
        except:
            sleep_if_needed()
        try:
            get_player_ShootingSplits(IDs['PLAYER_ID'], IDs['TEAM_ID'],IDs['Season'])
        except:
            sleep_if_needed()
        try:
            get_player_PerformanceSplits(IDs['PLAYER_ID'], IDs['TEAM_ID'],IDs['Season'])
        except:
            sleep_if_needed()
        try:
            get_player_YearOverYearSplits(IDs['PLAYER_ID'], IDs['TEAM_ID'],IDs['Season'])
        except:
            sleep_if_needed()
        try:
            get_player_Career(IDs['PLAYER_ID'], IDs['TEAM_ID'],IDs['Season'])
        except:
            sleep_if_needed()
        try:
            get_player_Profile(IDs['PLAYER_ID'], IDs['TEAM_ID'],IDs['Season'])
        except:
            sleep_if_needed()
        try:
            get_player_ShotTraking(IDs['PLAYER_ID'], IDs['TEAM_ID'],IDs['Season'])
        except:
            sleep_if_needed()
        try:
            get_player_ReboundTracking(IDs['PLAYER_ID'], IDs['TEAM_ID'],IDs['Season'])
        except:
            sleep_if_needed()
        try:
            get_player_PassTracking(IDs['PLAYER_ID'], IDs['TEAM_ID'],IDs['Season'])
        except:
            sleep_if_needed()

    elapsed_time = time.time() - start_time
    print('total elapsed time : ', elapsed_time, ' sec')

    conn.close()


def main():
    # When you start this application, you can type season that you want to pull the data (e.g. 2017-18)
    pull_current(sys.argv[0])

if __name__ == "__main__":
    main()

