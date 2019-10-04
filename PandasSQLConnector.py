import json
from pandas.io import sql
import pandas
from sqlalchemy import create_engine
import mysql.connector

class SQLHelper(object):
    def __init__(self):
        with(open(r'C:\Users\Herby\Documents\Python\Baseball Stuff\mysql.json')) as f:
                    dbconfig = json.load(f)
        self.engine = create_engine("mysql://" + dbconfig['database']['user'] + ":" + dbconfig['database']['passwd'] + "@" + dbconfig['database']['host'] + ":" + str(dbconfig['database']['port']) + "/" + dbconfig['database']['db'])
        
    def insertDataframe(self, data):
        try:
            data.to_sql("Pitches",self.engine,if_exists='append',index=False)
        except Exception as e:
            print(str(e))