import csv
import urllib.request
import io
from SQLConnector import SQLHelper

def getSavantData(team, year, playerType):
    url = "https://baseballsavant.mlb.com/statcast_search/csv?hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfGT=R%7C&hfC=&hfSea=" + year + "%7C&hfSit=&player_type=" + playerType + "&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt=&game_date_lt=&team=" + team + "&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_abs=0#results"
    response = urllib.request.urlopen(url)
    return response.read().decode()

def makeRows(data):
    lines = data.splitlines()
    rows = []
    for r in lines:
        rows.append(list(csv.reader(io.StringIO(r))))
    return rows

SQLHelper = SQLHelper()
teams = ['LAA', 'HOU', 'OAK', 'TOR', 'ATL', 'MIL', 'STL',
         'CHC', 'ARI', 'LAD', 'SF', 'CLE', 'SEA', 'MIA',
         'NYM', 'WSH', 'BAL', 'SD', 'PHI', 'PIT', 'TEX',
         'TB', 'BOS', 'CIN', 'COL', 'KC', 'DET', 'MIN',
         'CWS', 'NYY']
years = ['2019']
playerTypes = ['batter', 'pitcher']

listPlayers = []
listIDs = []
idFromTable = SQLHelper.select("select PlayerID from Players")
for tableID in idFromTable:
    listIDs.append(int(tableID["PlayerID"]))

    
for team in teams:
    for playerType in playerTypes:
        for year in years:
            rows = makeRows(getSavantData(team, year, playerType))
            del rows[0]
            for r1 in rows:
                if int(r1[0][1]) not in listIDs:
                    name = r1[0][2].split(" ", 1)
                    firstName = name[0]
                    lastName = name[1]
                    listPlayers.append([r1[0][1], firstName, lastName])
                    listIDs.append(int(r1[0][1]))

stmt = "insert into Players (PlayerID,FirstName,LastName) values (%s,%s,%s)"
SQLHelper.insertMany(stmt, listPlayers)
