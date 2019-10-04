import csv
import requests
import io
import pandas as pd
import numpy as np
from PandasSQLConnector import SQLHelper
from datetime import datetime, timedelta

def getSavantData(team,date):
    #url = "https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfGT=R%7C&hfC=&hfSea=" + year + "%7C&hfSit=&player_type=batter&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt=&game_date_lt=&hfInfield=&team=" + team + "&position=&hfOutfield=&hfRO=&home_road=&hfFlag=&hfPull=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_pas=0&type=details&"
    url = "https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfGT=R%7C&hfC=&hfSea=2019%7C&hfSit=&player_type=batter&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt=" + date + "&game_date_lt=" + date + "&hfInfield=&team=" + team + "&position=&hfOutfield=&hfRO=&home_road=&hfFlag=&hfPull=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_pas=0&type=details&"
    s = requests.get(url).content
    return pd.read_csv(io.StringIO(s.decode("utf-8")))

def removeColumns(data):
    columnsNeeded = ['pitch_type','game_date','release_speed','batter', 'pitcher', 'events', 'description', 'zone','stand', 'p_throws', 'home_team', 'away_team','hit_location', 'bb_type', 'balls', 'strikes', 'on_3b', 'on_2b', 'on_1b', 'outs_when_up','inning', 'inning_topbot',  'hit_distance_sc', 'launch_speed','launch_angle', 'effective_speed', 'release_spin_rate','estimated_ba_using_speedangle', 'estimated_woba_using_speedangle','launch_speed_angle','at_bat_number', 'pitch_number', 'pitch_name','if_fielding_alignment', 'of_fielding_alignment']
    for col in data.columns.values:
        if col not in columnsNeeded:
            del data[col]    
    return data

def addNewColumns(data):
    data.insert(data.columns.get_loc("game_date"),'PitchTypeID',"")
    data.insert(data.columns.get_loc("events"),'EventID',"")
    data.insert(data.columns.get_loc("bb_type"),'BattedBallID',"")
    data.insert(data.columns.get_loc("balls"),'CountID',"")
    data.insert(data.columns.get_loc("on_3b"),'On3B',"")
    data.insert(data.columns.get_loc("on_2b"),'On2B',"")
    data.insert(data.columns.get_loc("on_1b"),'On1B',"")  
    data.rename(columns={'pitch_type':'PitchType','game_date':'GameDate','release_speed':'ReleaseSpeed','batter':'BatterID','pitcher':'PitcherID','events':'EventName','description':'EventDescription','zone':'PitchZone','stand':'BatterStand','p_throws':'PitcherThrows','home_team':'HomeTeam','away_team':'AwayTeam','hit_location':'HitLocation','bb_type':'BattedBallType','balls':'Balls','strikes':'Strikes','on_3b':'On3BID','on_2b':'On2BID','on_1b':'On1BID','outs_when_up':'OutsWhenUp','inning':'Inning','inning_topbot':'InningTopBot','hit_distance_sc':'HitDistance','launch_speed':'LaunchSpeed','launch_angle':'LaunchAngle','effective_speed':'EffectiveSpeed','release_spin_rate':'ReleaseSpinRate','estimated_ba_using_speedangle':'EstimatedBA_Speedangle','estimated_woba_using_speedangle':'EstimatedwOBA_Speedangle','launch_speed_angle':'LaunchSpeedAngle','at_bat_number':'ABNumber','pitch_number':'PitchNumber','pitch_name':'PitchName','if_fielding_alignment':'IFAlignment','of_fielding_alignment':'OFAlignment'}, inplace=True)  
    return data

def addNewIDs(data):
    for i, row in data.iterrows():
        data.at[i,'PitchTypeID'] = getPitchTypeID(row["PitchType"])
        data.at[i,'EventID'] = getEventID(row["EventName"])
        data.at[i,'BattedBallID'] = getBattedBallID(row["BattedBallType"])
        data.at[i, 'CountID'] = getCountID(row["Balls"], row["Strikes"])
        data.at[i,'IFAlignment'] = getIFAlignment(row["IFAlignment"])
        data.at[i,'OFAlignment'] = getOFAlignment(row["OFAlignment"])
    return data

def getPitchTypeID(pitchType):
    return {
        'FT':1, 'SL':2, 'FF':3, 'CH':4, 'CU':5, 'FC':6, 'KC':7, 'SI':8, 'IN':9, 'PO':10, 'KN':11, 'EP':12, 'FS':13, 'FO':14, 'SC':15, 'UN':16
    }.get(pitchType, 0)

def getEventID(event):
    return {
        'field_out':1, 'strikeout':2, 'walk':3, 'home_run':4, 'single':5, 'double':6, 'fielders_choice_out':7, 'sac_fly':8, 'hit_by_pitch':9, 'caught_stealing_2b':10, 'field_error':11, 'force_out':12,
        'grounded_into_double_play':13, 'triple':14, 'other_out':15, 'intent_walk':16, 'double_play':17, 'sac_bunt':18, 'catcher_interf':19, 'sac_fly_double_play':20, 'strikeout_double_play':21, 'fielders_choice':22,
        'pickoff_2b':23, 'pickoff_caught_stealing_2b':24, 'triple_play':25, 'pickoff_1b':26, 'pickoff_caught_stealing_3b':27, 'batter_interference':28, 'caught_stealing_3b':29, 'caught_stealing_home':30, 'run':31,
        'sac_bunt_double_play':32, 'pickoff_caught_stealing_home':33, 'pickoff_3b':34, 'intentional_walk':35
    }.get(event, 0)

def getBattedBallID(bbType):
    return {
        'popup':1, 'fly_ball':2, 'ground_ball':3, 'line_drive':4
    }.get(bbType, 0)

def getCountID(balls, strikes):
    count = str(balls) + str(strikes)
    return {
        '00':1, '01':2, '02':3, '10':4, '11':5, '12':6, '20':7, '21':8, '22':9, '30':10, '31':11, '32':12  
    }.get(count, 0)

def getIFAlignment(IFAlignment):
    return {
        'Infield shift':1, 'Standard':2, 'Strategic':3
    }.get(IFAlignment, 0)

def getOFAlignment(OFAlignment):
    return {
        'Standard':1, 'Strategic':2, '4th outfielder':4, 'Extreme outfield shift':5
    }.get(OFAlignment, 0)

#initial data needed for main program

teams = ['LAA', 'HOU', 'OAK', 'TOR', 'ATL', 'MIL', 'STL',
         'CHC', 'ARI', 'LAD', 'SF', 'CLE', 'SEA', 'MIA',
         'NYM', 'WSH', 'BAL', 'SD', 'PHI', 'PIT', 'TEX',
         'TB', 'BOS', 'CIN', 'COL', 'KC', 'DET',
         'CWS', 'NYY']

sqlh = SQLHelper()

yesterdayDate = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

for team in teams:
    data = getSavantData(team, yesterdayDate)
    data = removeColumns(data)
    data = addNewColumns(data)
    data = addNewIDs(data)
    sqlh.insertDataframe(data)