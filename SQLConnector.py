import mysql.connector
import json

class SQLHelper(object):

    def __init__(self):
        with(open(r'C:\Users\Herby\Documents\Python\Baseball Stuff\mysql.json')) as f:
            dbconfig = json.load(f)

        self.cnx = mysql.connector.connect(user=dbconfig['database']['user'],
                                      password=dbconfig['database']['passwd'],
                                      host=dbconfig['database']['host'],
                                      database=dbconfig['database']['db'],
                                      port=dbconfig['database']['port'])

    def insert(self, query):
        try:
            cursor = self.cnx.cursor()
            cursor.execute(query)
            self.cnx.commit()
        except Exception as e:
            print(str(e))
        cursor.close()

    def select(self, query):
        try:
            cursor = self.cnx.cursor()
            cursor.execute(query)
            columns = cursor.description
            return [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
        except Exception as e:
            print(str(e))
        cursor.close()

    def insertMany(self, query, data):
        try:
            cursor = self.cnx.cursor()
            cursor.executemany(query, data)
            self.cnx.commit()
        except Exception as e:
            print(str(e))
        cursor.close()

    def __del__(self):
        self.cnx.close()
